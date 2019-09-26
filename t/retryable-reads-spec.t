#  Copyright 2018 - present MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

use strict;
use warnings;
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Deep ':v1';
use Safe::Isa;
use JSON::MaybeXS qw( is_bool decode_json );
use Storable qw( dclone );
use BSON::Types ':all';

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    clear_testdbs
    get_unique_collection
    server_version
    server_type
    check_min_server_version
    skip_unless_mongod
    skip_unless_sessions
    skip_unless_failpoints_available
    to_snake_case
    remap_hashref_to_snake_case
    get_features
    set_failpoint
    clear_failpoint
/;
use MongoDBSpecTest qw/
    foreach_spec_test
    skip_unless_run_on
    maybe_skip_multiple_mongos
/;

skip_unless_mongod(v3.6.0);
skip_unless_failpoints_available();

# Increase wtimeout much higher for CI dropping database issues
my $conn           = build_client( wtimeout => 60000 );

my @events;
sub clear_events { @events = () }

sub event_cb { push @events, dclone $_[0] }

my $db;
foreach_spec_test('t/data/retryable-reads', $conn, sub {
    my ($test, $plan) = @_;
    maybe_skip_multiple_mongos( $conn, $test->{useMultipleMongoses} );

    TODO: {
        todo_skip('PERL-589: GridFSBucket download', 1)
            if $test->{'description'} =~ /DownloadByName|download_by_name/i;

        my $client_options = $test->{'clientOptions'};
        $client_options = remap_hashref_to_snake_case( $client_options );
        $client_options->{'monitoring_callback'} = \&event_cb;
        my $client = build_client(%$client_options);

        $db->drop if defined $db;
        ok($db = get_test_db($conn), 'got test db');
        ok($db = $client->get_database($db->name), 'got client test db');
        $db->run_command([ create => $plan->{'database_name'} ]);
        my ($coll, $gridfs);
        if (exists $plan->{'collection_name'}) {
            ok($coll = $db->get_collection($plan->{'collection_name'}),
               'got collection');
            $coll->drop;
            $coll->insert_many($plan->{'data'});
        }
        elsif (exists $plan->{'bucket_name'}) {
            ok($gridfs = $db->gfs({ bucket_name => $plan->{'bucket_name'} }),
               'got bucket');
            $gridfs->drop;

            my $files = $db->get_collection('fs.files');
            $files->drop;
            my $files_data = $plan->{'data'}{'fs.files'};
            $files_data->[0]{'_id'} = bson_oid($files_data->[0]{'_id'}{'$oid'})
                unless $files_data->[0]{'_id'}->$_isa('BSON::OID');
            $files->insert_many($files_data);

            my $chunks = $db->get_collection('fs.chunks');
            $chunks->drop;
            my $chunks_data = $plan->{'data'}{'fs.chunks'};
            $chunks_data->[0]{'_id'} = bson_oid($chunks_data->[0]{'_id'}{'$oid'})
                unless $chunks_data->[0]{'_id'}->$_isa('BSON::OID');
            $chunks->insert_many($chunks_data);
        }

        set_failpoint( $client, $test->{'failPoint'} );
        clear_events();
        foreach my $op (@{ $test->{'operations'} || [] }) {
            my $method = $op->{'name'};
            $method =~ s{([A-Z])}{_\L$1}g;
            my $func_name = 'do_' . $method;
            my $ret = eval {
                main->$func_name( $coll || $gridfs, $op->{'arguments'}, $op->{'object'} )
            };
            my $err = $@;
            if ($op->{'error'}) {
                ok $err, 'Exception occured';
            }
            elsif ($err && $err !~ /failpoint/) {
                return fail($err);
            }
            elsif ($op->{'result'}) {
                cmp_deeply($ret, $op->{'result'}, "checking result for $method")
                    or diag explain $ret;
            }
        }

        check_event_expectations(
            _adjust_types($test->{'expectations'}),
        );

        clear_failpoint( $client, $test->{'failPoint'} );
    }
});

sub _adjust_types {
    my ($value) = @_;
    if (ref $value eq 'HASH') {
        if (scalar(keys %$value) == 1) {
            my ($name, $value) = %$value;
            if ($name eq '$numberLong') {
                return 0+$value;
            }
            if ($name eq '$oid') {
                my $id = bson_oid($value);
                ok($id->hex, 'check hex value of $oid');
                return $id;
            }
        }
        return +{map {
            my $key = $_;
            ($key, _adjust_types($value->{$key}));
        } keys %$value};
    }
    elsif (ref $value eq 'ARRAY') {
        return [map { _adjust_types($_) } @$value];
    }
    else {
        return $value;
    }
}

sub prepare_data_spec {
    my ($spec) = @_;
    if (is_bool $spec) {
        my $specced = $spec ? 1 : 0;
        return code(sub {
            my $value = shift;
            return(0, 'expected a true boolean value')
                if $specced and not $value;
            return(0, 'expected a false boolean value')
                if $value and not $specced;
            return 1;
        });
    }
    elsif (ref $spec eq 'ARRAY') {
        return [map {
            prepare_data_spec($_)
        } @$spec];
    }
    elsif (ref $spec eq 'HASH') {
        return +{map {
            ($_, prepare_data_spec($spec->{$_}))
        } keys %$spec};
    }
    else {
        return $spec;
    }
}

sub check_event_expectations {
    my ($expected) = @_;
    my @got =
        grep { $_->{'commandName'} !~ /configureFailPoint|sasl|ismaster|kill|getMore|insert/ }
        grep { ($_->{'type'}||q{}) eq 'command_started' }
        @events;
    for my $exp ( @$expected ) {
        my ($exp_type, $exp_spec) = %$exp;
        subtest $exp_type => sub {
            ok(scalar(@got), 'event available')
                or return;
            my $event = shift @got;
            is($event->{type}.'_event', $exp_type, "is a $exp_type")
                or return;
            my $event_tester = "check_$exp_type";
            main->can($event_tester)->($exp_spec, $event);
        };
    }
    is(scalar(@got), 0, 'no outstanding events');
}

sub check_event {
    my ($exp, $event) = @_;
    for my $key (sort keys %$exp) {
        my $check = "check_${key}_field";
        main->can($check)->($exp->{$key}, $event);
    }
}

sub check_command_started_event {
    my ($exp, $event) = @_;
    check_event($exp, $event);
}

sub check_command_succeeded_event {
    my ($exp, $event) = @_;
    check_event($exp, $event);
}

sub check_command_failed_event {
    my ($exp, $event) = @_;
    check_event($exp, $event);
}

sub check_database_name_field {
    my ($exp_name, $event) = @_;
    ok defined($event->{databaseName}), "database_name defined";
    ok length($event->{databaseName}), "database_name non-empty";
}

sub check_command_name_field {
    my ($exp_name, $event) = @_;
    is $event->{commandName}, $exp_name, "command name";
}

sub check_command_field {
    my ($exp_command, $event) = @_;
    my $event_command = $event->{command};
    for my $exp_key (sort keys %$exp_command) {
        my $exp_value = prepare_data_spec($exp_command->{$exp_key});
        if ($exp_key =~ /listIndexNames/) {
            $exp_key = 'listIndexes';
        }
        my $event_value = $event_command->{$exp_key};
        my $label = "command field '$exp_key'";
        if ( ref $event_value eq 'HASH' ) {
            if (exists $event_value->{'_id'} || exists $event_value->{'files_id'}) {
                my $got_id = $event_value->{'_id'} || $event_value->{'files_id'};
                my $exp_id = $exp_value->{'_id'} || $exp_value->{'files_id'};
                if ( $got_id->$_isa('BSON::OID') ) {
                    is($got_id->hex, $exp_id->hex, 'check hex value');
                }
            }
        }
        cmp_deeply $event_value, $exp_value, $label
            or diag explain $event_command;
    }
}

sub do_aggregate {
    my ($main, $coll, $args) = @_;
    return [ $coll->aggregate($args->{'pipeline'})->all ];
}

sub do_watch {
    my ($main, $coll, $args, $on) = @_;
    my $obj_map = {
        collection => sub { $coll },
        client => sub { $coll->client },
        database => sub { $coll->database },
    };
    return $obj_map->{$on}->()->watch;
}

sub do_distinct {
    my ($main, $coll, $args) = @_;
    return [
        $coll->distinct($args->{'fieldName'}, $args->{'filter'})->all
    ];
}

sub do_find {
    my ($main, $coll, $args) = @_;
    my $cursor = $coll->find($args->{'filter'}, {
        map { $_ => $args->{$_} } qw(sort limit)
    });
    return [ $cursor->all ];
}

sub do_find_one {
    my ($main, $coll, $args) = @_;
    return $coll->find_one($args->{'filter'});
}

sub do_estimated_document_count {
    my ($main, $coll, $args) = @_;
    return $coll->estimated_document_count;
}

sub do_count_documents {
    my ($main, $coll, $args) = @_;
    return $coll->count_documents($args->{'filter'});
}

sub do_count {
    my ($main, $coll, $args) = @_;
    return $coll->count_documents($args->{'filter'});
}

sub do_list_collection_names {
    my ($main, $coll, $args) = @_;
    return [ $coll->database->collection_names ];
}

sub do_list_collection_objects {
    my ($main, $coll, $args) = @_;
    return $main->do_list_collections($coll, $args);
}

sub do_list_collections {
    my ($main, $coll, $args) = @_;
    return [ $coll->database->list_collections->all ];
}

sub do_list_database_objects {
    my ($main, $coll, $args) = @_;
    return $main->do_list_databases($coll, $args);
}

sub do_list_database_names {
    my ($main, $coll, $args) = @_;
    return [ $coll->client->database_names ];
}

sub do_list_databases {
    my ($main, $coll, $args) = @_;
    return [ $coll->client->list_databases ];
}

sub do_list_index_names {
    my ($main, $coll, $args) = @_;
    return [ map { $_->{'name'} } @{$main->do_list_indexes($coll, $args)} ];
}

sub do_list_indexes {
    my ($main, $coll, $args) = @_;
    $coll->insert_one({});
    return [ $coll->indexes->list->all ];
}

sub do_download {
    my ($main, $gridfs, $args) = @_;
    my $stream = $gridfs->open_download_stream(
        bson_oid($args->{'id'}{'$oid'})
    );
    my $data = do { local $/; $stream->readline };
    $stream->close;
    return $data;
}

clear_testdbs;

done_testing;
