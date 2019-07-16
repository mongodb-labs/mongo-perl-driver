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
use utf8;
use Test::More 0.96;
use Test::Deep;
use Safe::Isa;
use Storable qw( dclone );

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    skip_unless_failpoints_available
    set_failpoint
    clear_failpoint
    check_min_server_version
/;
use MongoDBSpecTest qw/foreach_spec_test/;

skip_unless_mongod();
skip_unless_failpoints_available();

my @events;

sub clear_events { @events = () }

sub event_cb { push @events, dclone $_[0] }

my $global_client = build_client();
my $server_version = server_version($global_client);
my $server_type = server_type($global_client);
my $server_topology =
    $server_type eq 'RSPrimary' ? 'replicaset' :
    $server_type eq 'Standalone' ? 'single' :
    'unknown';

my ($db1, $db2);

foreach_spec_test('t/data/change-streams', $global_client, sub {
    my ($test, $plan) = @_;

    plan skip_all => sprintf(
        "Test only runs on (%s) topology",
        join('|', @{ $test->{topology} || [] }),
    ) unless grep { $_ eq $server_topology } @{ $test->{topology} || [] };

    my $min_version = defined($test->{minServerVersion})
        ? version->parse('v'.$test->{minServerVersion})
        : undef;
    plan skip_all => "Test requires version $min_version"
        if defined($min_version) and check_min_server_version($global_client, $min_version);

    $db1->drop if defined $db1;
    $db2->drop if defined $db2;

    $db1 = get_test_db($global_client);
    $db2 = get_test_db($global_client);

    my $client = build_client(monitoring_callback => \&event_cb);
    set_failpoint($client, $test->{'failPoint'});
    clear_events();

    $db1 = $client->get_database($db1->name);
    $db2 = $client->get_database($db2->name);

    $db1->run_command([create => $plan->{database_name}]);
    $db2->run_command([create => $plan->{database2_name}]);

    my $coll = $db1->get_collection($plan->{collection_name});
    $coll->drop;
    if ($test->{description} =~ /rename|drop/i) {
        # lets add bogus document in order to avoid 'invalid source ns' error
        $coll->insert_one({});
    }

    my $stream_target =
        $test->{target} eq 'collection' ? $coll :
        $test->{target} eq 'database' ? $db1 :
        $test->{target} eq 'client' ? $client :
        die "Unknown target: ".$test->{target};

    my $resolve_db = sub {
        $_[0] eq $plan->{database_name} ? $db1 :
        $_[0] eq $plan->{database2_name} ? $db2 :
        undef
    };

    my $stream;
    eval {
        $stream = $stream_target->watch(
            $test->{changeStreamPipeline} || [],
            $test->{changeStreamOptions} || {},
        );
    };
    my $stream_error = $@;
    if ($stream_error) {
        ok(defined($test->{result}{error}), 'expected error')
            or diag("Stream Error: $stream_error");
        if (defined(my $code = $test->{result}{error}{code})) {
            is $stream_error->code, $code, "error code $code";
            if (my $err_labels = $test->{result}{error}{'errorLabels'}) {
                cmp_deeply($stream_error->error_labels, $err_labels, 'errorLabels');
            }
        }
    }
    else {
        ok(defined($stream), 'change stream')
            or return;
    }

    for my $operation (@{ $test->{operations} || [] }) {
        my ($op_db, $op_coll, $op_name)
            = @{ $operation }{qw( database collection name )};
        $op_db = $op_db->$resolve_db;
        my $orig_coll_name = $op_coll;
        $op_coll = $op_db->get_collection($op_coll);

        my $op_sub = __PACKAGE__->can("operation_${op_name}");
        $op_sub->($op_db, $op_coll, $operation->{arguments});
    }

    if (my @expected_events = @{ $test->{expectations} || [] }) {
        subtest 'events' => sub {
            my $index = 0;
            for my $expected (@expected_events) {
                $index++;
                my $found;
                subtest "event (index $index)" => sub {
                    while (@events) {
                        my $current = shift @events;
                        my $data = event_matches($current, $expected);
                        if (defined $data) {
                            check_event(
                                $current,
                                prepare_spec($data, $resolve_db),
                                $resolve_db,
                            );
                            return;
                        }
                    }
                    ok 0, 'missing expected event';
                };
            }
        };
    }
    clear_failpoint($client, $test->{'failPoint'});

    my $test_result = $test->{result};
    if ($test_result->{success}) {
        subtest 'success' => sub {
            my @changes;
            while (defined(my $change = $stream->next)) {
                push @changes, $change;
            }
            my @expected_changes = @{ $test_result->{success} };
            is scalar(@changes), scalar(@expected_changes),
                'expected number';
            if (@changes == @expected_changes) {
                for my $index (0 .. $#changes) {
                    subtest "result (index $index)" => sub {
                        check_result(
                            $changes[$index],
                            prepare_spec(
                                $expected_changes[$index],
                                $resolve_db,
                            ),
                            $resolve_db
                        );
                    };
                }
            }
            else {
                fail(
                    sprintf(
                        'Expected (%d) changes, but got (%d)',
                        scalar(@expected_changes),
                        scalar(@changes)
                    )
                );
            }
        };
    }
    elsif (my $test_err = $test_result->{error}) {
        if ($stream) {
            my $change = eval { $stream->next };
            my $err = $@;
            cmp_deeply($err->error_labels, $test_err->{'errorLabels'}, 'errorLabels');
        }
    }
});

sub event_matches {
    my ($event, $expected) = @_;

    my $data;
    if ($data = $expected->{command_started_event}) {
        return undef
            unless ($event->{type} // '') eq 'command_started';
    }
    elsif ($data = $expected->{command_succeeded_event}) {
        return undef
            unless ($event->{type} // '') eq 'command_succeeded';
    }
    else {
        die "Unrecognized event";
    }

    return undef
        unless $event->{commandName} eq $data->{command_name};

    return $data;
}

sub prepare_spec {
    my ($data, $resolve_db) = @_;
    if (not defined $data) {
        return undef;
    }
    elsif ($data->$_isa('JSON::PP::Boolean')) {
        my $value = !!$data;
        return code(sub {
            ($_[0] and $value) ? (1) :
            (!$_[0] and !$value) ? (1) :
            (0, "boolean mismatch")
        });
    }
    elsif ($data eq 42) {
        return code(sub {
            defined($_[0])
                ? (1)
                : (0, 'value is defined');
        });
    }
    elsif (ref $data eq 'HASH') {
        if (exists $data->{'$numberInt'}) {
            return 0+$data->{'$numberInt'};
        }
        return +{map {
            ($_, prepare_spec($data->{$_}, $resolve_db));
        } keys %$data};
    }
    elsif (ref $data eq 'ARRAY') {
        return [map {
            prepare_spec($_, $resolve_db);
        } @$data];
    }
    elsif (
        not ref $data
        and defined $data
        and defined(my $real_db = $data->$resolve_db)
    ) {
        return $real_db->name;
    }
    else {
        return $data;
    }
}

sub check_event {
    my ($event, $expected) = @_;

    is $event->{databaseName}, $expected->{database_name},
        'database name',
        if exists $expected->{database_name};

    if (my $command = $expected->{command}) {
        for my $key (sort keys %$command) {
            cmp_deeply(
                ($event->{command} || $event->{reply})->{$key},
                $command->{$key},
                $key,
            );
        }
    }
}

sub check_result {
    my ($change, $expected, $resolve_db) = @_;

    for my $key (sort keys %$expected) {
        if ($key eq 'fullDocument') {
            for my $doc_key (sort keys %{ $expected->{$key} }) {
                cmp_deeply(
                    $change->{$key}{$doc_key},
                    $expected->{$key}{$doc_key},
                    "$key/$doc_key",
                );
            }
        }
        else {
            cmp_deeply($change->{$key}, $expected->{$key}, $key);
        }
    }
}

sub operation_insertOne {
    my ($db, $coll, $args) = @_;
    $coll->insert_one($args->{document});
}

sub operation_updateOne {
    my ($db, $coll, $args) = @_;
    $coll->update_one($args->{filter}, $args->{update});
}

sub operation_replaceOne {
    my ($db, $coll, $args) = @_;
    $coll->replace_one($args->{filter}, $args->{replacement});
}

sub operation_deleteOne {
    my ($db, $coll, $args) = @_;
    $coll->delete_one($args->{filter});
}

sub operation_rename {
    my ($db, $coll, $args) = @_;
    my $new_name = $args->{'to'};
    $coll->rename($new_name);
}

sub operation_drop {
    my ($db, $coll, $args) = @_;
    $coll->drop;
}

done_testing;
