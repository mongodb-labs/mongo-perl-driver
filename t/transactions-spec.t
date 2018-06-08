#
#  Copyright 2015 MongoDB, Inc.
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
#

use strict;
use warnings;
use JSON::MaybeXS qw( is_bool decode_json );
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.96;
use Test::Deep;
use Math::BigInt;

use utf8;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
    skip_unless_mongod
    skip_unless_failpoints_available
/;

skip_unless_mongod();
# TODO skip_unless_failpoints_available();

my @events;

#use Devel::Dwarn;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }#; Dwarn $_[0] }

my $conn           = build_client();
my $server_version = server_version($conn);
my $server_type    = server_type($conn);

plan skip_all => "Requires MongoDB 4.0"
    if $server_version < v4.0.0;

plan skip_all => "deployment does not support transactions"
    unless $conn->_topology->_supports_transactions;

# defines which argument hash fields become positional arguments
my %method_args = (
    insert_one  => [qw( document )],
    insert_many => [qw( documents )],
    delete_one  => [qw( filter )],
    delete_many => [qw( filter )],
    replace_one => [qw( filter replacement )],
    update_one  => [qw( filter update )],
    update_many => [qw( filter update )],
    find        => [qw( filter )],
    count       => [qw( filter )],
    bulk_write  => [qw( requests )],
);

my $dir      = path("t/data/transactions");
my $iterator = $dir->iterator; my $index = 0; # TBSLIVER
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/; next unless ++$index == 3; # TBSLIVER
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    my $test_db_name = $plan->{database_name};
    my $test_coll_name = $plan->{collection_name};

    subtest $path => sub {

        for my $test ( @{ $plan->{tests} }[0] ) { # TBSLIVER
            my $description = $test->{description};
            subtest $description => sub {
                my $client = build_client();

                # Kills its own session as well
                eval { $client->send_admin_command([ killAllSessions => [] ]) };
                my $test_db = $client->get_database( $test_db_name );

                # We crank wtimeout up to 10 seconds to help reduce
                # replication timeouts in testing
                $test_db->get_collection(
                    $test_coll_name,
                    { write_concern => { w => 'majority', wtimeout => 10000 } }
                )->drop;

                # Drop first to make sure its clear for the next test.
                # MongoDB::Collection doesnt have a ->create option so done as
                # a seperate step.
                $test_db->run_command([ create => $test_coll_name ]);

                my $test_coll = $test_db->get_collection(
                    $test_coll_name,
                    { write_concern => { w => 'majority', wtimeout => 10000 } }
                );

                if ( scalar @{ $plan->{data} } > 0 ) {
                    $test_coll->insert_many( $plan->{data} );
                }

                set_failpoint( $client, $test->{failPoint} );
                run_test( $test_db_name, $test_coll_name, $test );
                clear_failpoint( $client, $test->{failPoint} );
            };
        }
    };
}

sub set_failpoint {
    my ( $client, $failpoint ) = @_;

    return unless defined $failpoint;
    $client->send_admin_command([
        configureFailPoint => $failpoint->{configureFailPoint},
        mode => $failpoint->{mode},
        defined $failpoint->{data}
          ? ( data => $failpoint->{data} )
          : (),
    ]);
}

sub clear_failpoint {
    my ( $client, $failpoint ) = @_;

    return unless defined $failpoint;
    $client->send_admin_command([
        configureFailPoint => $failpoint->{configureFailPoint},
        mode => 'off',
    ]);
}

sub to_snake_case {
  my $t = shift;
  $t =~ s{([A-Z])}{_\L$1}g;
  return $t;
}

# Global so can get values when checking sessions
my %sessions;

sub run_test {
    my ( $test_db_name, $test_coll_name, $test ) = @_;

    my $client_options = $test->{clientOptions} // {};
    # Remap camel case to snake case
    $client_options = {
      map {
        my $k = to_snake_case( $_ );
        $k => $client_options->{ $_ }
      } keys %$client_options
    };

    my $client = build_client( monitoring_callback => \&event_cb, %$client_options );

    my $session_options = $test->{sessionOptions} // {};

    %sessions = (
      session0 => $client->start_session( $session_options->{session0} ),
      session1 => $client->start_session( $session_options->{session1} ),
    );
    $sessions{session0_lsid} = $sessions{session0}->session_id;
    $sessions{session1_lsid} = $sessions{session1}->session_id;

    # Cant see any in the files?
    my $collection_options = $test->{collectionOptions} // {};

    clear_events();
    for my $operation ( @{ $test->{operations} } ) {
        eval {
            my $test_db = $client->get_database( $test_db_name );
            my $test_coll = $test_db->get_collection( $test_coll_name, $collection_options );
            my $cmd = to_snake_case( $operation->{name} );

            diag $cmd;
            #Dwarn $operation;
            if ( $cmd =~ /_transaction$/ ) {
                $sessions{ $operation->{object} }->$cmd;
            } else {
                my @args = _adjust_arguments( $cmd, $operation->{arguments} );
                $args[-1]->{session} = $sessions{ $args[-1]->{session} }
                    if defined $args[-1]->{session};

                $test_coll->$cmd( @args );
            }
        };
        #Dwarn '----------------Session------------------';
        #Dwarn $sessions{session0}->_debug;
        my $err = $@;
        if ( $err ) {
          #Dwarn '----------------Error------------------';
          #Dwarn $err;
            my $err_contains        = $operation->{result}->{errorContains};
            my $err_code_name       = $operation->{result}->{errorCodeName};
            my $err_labels_contains = $operation->{result}->{errorLabelsContain};
            my $err_labels_omit     = $operation->{result}->{errorLabelsOmit};
            if ( defined $err_contains ) {
                like $err->message, qr/$err_contains/i, 'error contains' . $err_contains;
            }
            if ( defined $err_code_name ) {
                is $err->result->output->{codeName},
                   $err_code_name,
                   'error has name ' . $err_code_name;
            }
            if ( defined $err_labels_omit ) {
                for my $err_label ( @{ $err_labels_omit } ) {
                    ok ! $err->has_error_label( $err_label ), 'error doesnt have label ' . $err_label;
                }
            }
            if ( defined $err_labels_omit ) {
                for my $err_label ( @{ $err_labels_contains } ) {
                    ok $err->has_error_label( $err_label ), 'error has label ' . $err_label;
                }
            }
        } elsif ( grep {/^error/} keys %{ $operation->{result} } ) {
            ok 0, 'Should have found an error';
        }
    }

    $sessions{session0}->end_session;
    $sessions{session1}->end_session;

    #Dwarn \@events;
    if ( defined $test->{expectations} ) {
        check_event_expectations( _adjust_types( $test->{expectations} ) );
    }
    ok 1;
}

# Following subs modified from monitoring_spec.t
#


# prepare collection method arguments
# adjusts data structures and extracts leading positional arguments
sub _adjust_arguments {
    my ($method, $args) = @_;

    $args = _adjust_types($args);
    my @fields = @{ $method_args{$method} };
    my @field_values = map {
        my $val = delete $args->{$_};
        # bulk write is special cased to reuse argument extraction
        ($method eq 'bulk_write' and $_ eq 'requests')
            ? _adjust_bulk_write_requests($val)
            : $val;
    } @fields;

    return(
        (grep { defined } @field_values),
        scalar(keys %$args) ? $args : (),
    );
}

# prepare bulk write requests for use as argument to ->bulk_write
sub _adjust_bulk_write_requests {
    my ($requests) = @_;

    return [map {
        # Different data structure in bulk writes compared to command_monitoring
        my $name = to_snake_case( $_->{name} );
        +{ $name => [_adjust_arguments($name, $_->{arguments})] };
    } @$requests];
}

# some type transformations
# turns { '$numberLong' => $n } into 0+$n
sub _adjust_types {
    my ($value) = @_;
    if (ref $value eq 'HASH') {
        if (scalar(keys %$value) == 1) {
            my ($name, $value) = %$value;
            if ($name eq '$numberLong') {
                return 0+$value;
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

# common overrides for event data expectations
sub prepare_data_spec {
    my ($spec) = @_;
    if ( ! defined $spec ) {
        return $spec;
    }
    elsif (not ref $spec) {
        if ($spec eq 'test') {
            return any(qw( test test_collection ));
        }
        if ($spec eq 'test-unacknowledged-bulk-write') {
            return code(\&_verify_is_nonempty_str);
        }
        if ($spec eq 'command-monitoring-tests.test') {
            return code(\&_verify_is_nonempty_str);
        }
        return $spec;
    }
    elsif (is_bool $spec) {
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
    my ( $expected ) = @_;
    my @got = grep { $_->{type} eq 'command_started' } @events;

    #Dwarn \@events;
    for my $exp ( @$expected ) {
        my ($exp_type, $exp_spec) = %$exp;
        # We only have command_started_event checks
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

    is scalar(@got), 0, 'no outstanding events';
}

sub check_event {
    my ($exp, $event) = @_;
    for my $key (sort keys %$exp) {
        my $check = "check_${key}_field";
        main->can($check)->($exp->{$key}, $event);
    }
}

#
# per-event type test handlers
#

sub check_command_started_event {
    my ($exp, $event) = @_;
    check_event($exp, $event);
}

#
# verificationi subs for use with Test::Deep::code
#

sub _verify_is_positive_num {
    my $value = shift;
    return(0, "error code is not defined")
        unless defined $value;
    return(0, "error code is not positive")
        unless $value > 1;
    return 1;
}

sub _verify_is_nonempty_str {
    my $value = shift;
    return(0, "error message is not defined")
        unless defined $value;
    return(0, "error message is empty")
        unless length $value;
    return 1;
}

#
# event field test handlers
#

# $event.database_name
sub check_database_name_field {
    my ($exp_name, $event) = @_;
    ok defined($event->{databaseName}), "database_name defined";
    ok length($event->{databaseName}), "database_name non-empty";
}

# $event.command_name
sub check_command_name_field {
    my ($exp_name, $event) = @_;
    is $event->{commandName}, $exp_name, "command name";
}

# $event.reply
sub check_reply_field {
    my ($exp_reply, $event) = @_;
    my $event_reply = $event->{reply};

    # special case for $event.reply.cursor.id
    if (exists $exp_reply->{cursor}) {
        if (exists $exp_reply->{cursor}{id}) {
            $exp_reply->{cursor}{id} = code(\&_verify_is_positive_num)
                if $exp_reply->{cursor}{id} eq '42';
        }
    }

    # special case for $event.reply.writeErrors
    if (exists $exp_reply->{writeErrors}) {
        for my $i ( 0 .. $#{ $exp_reply->{writeErrors} } ) {
            my $error = $exp_reply->{writeErrors}[$i];
            if (exists $error->{code} and $error->{code} eq 42) {
                $error->{code} = code(\&_verify_is_positive_num);
            }
            if (exists $error->{errmsg} and $error->{errmsg} eq '') {
                $error->{errmsg} = code(\&_verify_is_nonempty_str);
            }
            $exp_reply->{writeErrors}[$i] = superhashof( $error );
        }
    }

    # special case for $event.command.cursorsUnknown on killCursors
    if ($event->{commandName} eq 'killCursors'
        and defined $exp_reply->{cursorsUnknown}
    ) {
        for my $index (0 .. $#{ $exp_reply->{cursorsUnknown} }) {
            $exp_reply->{cursorsUnknown}[$index]
                = code(\&_verify_is_positive_num)
                if $exp_reply->{cursorsUnknown}[$index] eq 42;
        }
    }

    for my $exp_key (sort keys %$exp_reply) {
        cmp_deeply
            $event_reply->{$exp_key},
            prepare_data_spec($exp_reply->{$exp_key}),
            "reply field $exp_key" or diag explain $event_reply->{$exp_key};
    }
}

# $event.command
sub check_command_field {
    my ($exp_command, $event) = @_;
    my $event_command = $event->{command};

    # ordered defaults to true
    delete $exp_command->{ordered};

    # special case for $event.command.getMore
    if (exists $exp_command->{getMore}) {
        $exp_command->{getMore} = code(\&_verify_is_positive_num)
            if $exp_command->{getMore} eq '42';
    }

    # special case for $event.command.writeConcern.wtimeout
    if (defined $exp_command->{writeConcern}) {
        $exp_command->{writeConcern}{wtimeout} = ignore();
    }

    # special case for $event.command.cursors on killCursors
    if ($event->{commandName} eq 'killCursors'
        and defined $exp_command->{cursors}
    ) {
        for my $index (0 .. $#{ $exp_command->{cursors} }) {
            $exp_command->{cursors}[$index]
                = code(\&_verify_is_positive_num)
                if $exp_command->{cursors}[$index] eq 42;
        }
    }

    if ( defined $exp_command->{lsid} ) {
        # Stuff correct session id in
        $exp_command->{lsid} = $sessions{ $exp_command->{lsid} . '_lsid' };
    }

    if ( defined $exp_command->{readConcern} ) {
        $exp_command->{readConcern}{afterClusterTime} = Isa('BSON::Timestamp')
            if $exp_command->{readConcern}{afterClusterTime} eq '42';
    }

    if ( defined $exp_command->{txnNumber} ) {
        $exp_command->{txnNumber} = Math::BigInt->new($exp_command->{txnNumber});
    }

    #DwarnN $exp_command;
    #DwarnN $event_command;

    for my $exp_key (sort keys %$exp_command) {
        my $event_value = $event_command->{$exp_key};
        my $exp_value = prepare_data_spec($exp_command->{$exp_key});
        my $label = "command field '$exp_key'";

        if (
            (grep { $exp_key eq $_ } qw( comment maxTimeMS ))
            or
            ($event->{commandName} eq 'getMore' and $exp_key eq 'batchSize')
        ) {
            TODO: {
                local $TODO =
                    "Command field '$exp_key' requires other fixes";
                cmp_deeply $event_value, $exp_value, $label;
            }
        }
        elsif ( !defined $exp_value )
        {
            ok ! exists $event_command->{$exp_key}, $label . ' does not exist';
        }
        else {
            cmp_deeply $event_value, $exp_value, $label;
        }
    }
}

clear_testdbs;

done_testing;
