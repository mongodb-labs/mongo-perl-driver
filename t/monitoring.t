#
#  Copyright 2018-present MongoDB, Inc.
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
use Test::More;
use Test::Fatal;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest
  qw/skip_unless_mongod build_client get_test_db server_version server_type/;

skip_unless_mongod();

my $conn           = build_client();
my $server_version = server_version($conn);
my $server_type    = server_type($conn);

#--------------------------------------------------------------------------#
# Event callback for testing -- just closures over an array
#--------------------------------------------------------------------------#

my @events;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }

#--------------------------------------------------------------------------#
# Tests
#--------------------------------------------------------------------------#

subtest "Initialize client with monitoring callback" => sub {
    clear_events();
    my $mc = build_client( monitoring_callback => \&event_cb );
    $mc->monitoring_callback->( { hello => "world" } );
    is( event_count(),       1,       "got an event" );
    is( $events[0]->{hello}, "world", "correct event" );
};

subtest "run_command" => sub {
    clear_events();
    my $mc = build_client( monitoring_callback => \&event_cb, dt_type => undef );
    $mc->send_admin_command( [ ismaster => 1 ] );
    ok( event_count() >= 2, "got 2+ events" ) or return;

    subtest "command_started" => sub {
        my @started = grep { $_->{type} eq "command_started" } @events;
        ok( scalar @started >= 1, "command_success count" ) or return;

        # last command should be the one we ran
        my $last_start = $started[-1];
        my $ok   = 1;
        $ok &&= is( $last_start->{databaseName},      "admin",    "databaseName" );
        $ok &&= is( $last_start->{commandName},       "ismaster", "commandName" );
        $ok &&= is( $last_start->{command}{ismaster}, 1,          "command" );
        $ok &&= ok( defined $last_start->{requestId}, "requestId" );
        $ok &&= like( $last_start->{connectionId}, qr/^[^:]+:\d+$/, "connectionId" );
        diag explain $last_start unless $ok;
    };

    subtest "command_succeeded" => sub {
        my @success = grep { $_->{type} eq "command_succeeded" } @events;
        ok( scalar @success >= 1, "command_succeeded count" ) or return;

        # last command should be the one we ran
        my $last_success = $success[-1];
        my $ok = 1;
        $ok &&= is( $last_success->{databaseName},      "admin",    "databaseName" );
        $ok &&= is( $last_success->{commandName},       "ismaster", "commandName" );
        $ok &&= ok( defined $last_success->{requestId}, "requestId" );
        $ok &&= ok( $last_success->{durationSecs} > 0, "duration" );
        $ok &&= like( $last_success->{connectionId}, qr/^[^:]+:\d+$/, "connectionId" );
        diag explain $last_success unless $ok;
    };

    subtest "command_failed" => sub {
        clear_events();
        eval { $mc->send_admin_command( [ notarealcommand => 1 ] ) };
        ok( $@, "Got exception" );
        ok( event_count() >= 2, "got 2+ events" ) or return;

        my @failure = grep { $_->{type} eq "command_failed" } @events;
        ok( scalar @failure >= 1, "command_failed count" ) or return;

        # last command should be the one we ran
        my $last_failure = $failure[-1];
        my $ok = 1;
        $ok &&= is( $last_failure->{databaseName},      "admin",    "databaseName" );
        $ok &&= is( $last_failure->{commandName},       "notarealcommand", "commandName" );
        $ok &&= ok( defined $last_failure->{requestId}, "requestId" );
        $ok &&= ok( $last_failure->{durationSecs} > 0, "duration" );
        $ok &&= like( $last_failure->{connectionId}, qr/^[^:]+:\d+$/, "connectionId" );
        $ok &&= like( $last_failure->{failure}, qr/no such command/i, "failure" );
        $ok &&= isa_ok( $last_failure->{reply}, 'HASH', "reply");
        diag explain $last_failure unless $ok;
    };
};

subtest "write commands" => sub {
    clear_events();
    my $coll   = _coll_with_monitor( "test_write_events" );
    _test_writes($coll);
};

subtest "unack'd writes" => sub {
    clear_events();
    my $coll   = _coll_with_monitor( "test_write_events", { write_concern => { w => 0 } } );
    _test_writes($coll);
};

subtest "find and getMore" => sub {
    clear_events();
    my $coll   = _coll_with_monitor("test_read_events");

    $coll->insert_many( [ map { ; { x => $_ } } 1 .. 100 ] );
    # Clear after insert so we're only looking for find/getmore
    clear_events();

    my @docs = $coll->find( { x => { '$gt' => 10 } }, { batchSize => 30 } )->all;

    subtest "command_started" => sub {
        my @started = grep { $_->{type} eq "command_started" } @events;
        ok( scalar @started >= 2, "got events" );
        my $ok = 1;
        $ok &&= is( (scalar grep { $_->{commandName} eq 'find' } @started), 1, "find command" );
        $ok &&= is( (scalar grep { $_->{commandName} eq 'getMore' } @started), 3, "getMore commands" );
        diag explain \@started unless $ok;
    };

    subtest "command_succeeded" => sub {
        my @succeeded = grep { $_->{type} eq "command_succeeded" } @events;
        ok( scalar @succeeded >= 2, "got events" );
        my $ok = 1;
        $ok &&= is( (scalar grep { $_->{commandName} eq 'find' } @succeeded), 1, "find command" );
        $ok &&= is( (scalar grep { $_->{commandName} eq 'getMore' } @succeeded), 3, "getMore commands" );
        diag explain \@succeeded unless $ok;
    };

    subtest "command_failed" => sub {
        clear_events();
        eval { $coll->find( { x => { '$xxxx' => 10 } }, { batchSize => 30 } )->all };
        ok( $@, "Got exception" );

        my @failed = grep { $_->{type} eq "command_failed" } @events;
        ok( scalar @failed >= 1, "got events" );
        my $ok = 1;
        $ok &&= is( (scalar grep { $_->{commandName} eq 'find' } @failed), 1, "find command" );
        diag explain \@failed unless $ok;
    };
};

subtest "exceptions are command_failed" => sub {

    subtest 'insert' => sub {
        no warnings 'redefine';
        my $coll = _coll_with_monitor("test");
        $coll->insert_one({}); # force topology discovery
        my $err;
        {
            local *MongoDB::_Link::read = \&_throw_mock_network_error;
            clear_events();
            eval {$coll->insert_one({})};
            $err = $@;
        }
        # force reset topology status
        $coll->client->topology_status( refresh => 1 );

        ok( $err, "got exception" );
        my @failed = grep { $_->{type} eq "command_failed" } @events;
        ok( scalar @failed >= 1, "got events" );
        my $last_failure = $failed[-1];
        my $ok = 1;
        $ok &&= is( $last_failure->{commandName},       "insert", "commandName" );
        $ok &&= ok( defined $last_failure->{requestId}, "requestId" );
        $ok &&= ok( $last_failure->{durationSecs} > 0, "duration" );
        $ok &&= like( $last_failure->{connectionId}, qr/^[^:]+:\d+$/, "connectionId" );
        $ok &&= like( $last_failure->{failure}, qr/fake network error/, "failure msg" );
        $ok &&= isa_ok( $last_failure->{eval_error}, "MongoDB::NetworkError", "eval_error" );
        diag explain $last_failure unless $ok;
    };

    subtest "insert unack'd" => sub {
        no warnings 'redefine';
        my $coll = _coll_with_monitor("test", { write_concern => { w => 0 } });
        $coll->insert_one({}); # force topology discovery
        my $err;
        {
            local *MongoDB::_Link::write = \&_throw_mock_network_error;
            clear_events();
            eval {$coll->insert_one({})};
            $err = $@;
        }
        # force reset topology status
        $coll->client->topology_status( refresh => 1 );

        ok( $err, "got exception" );
        my @failed = grep { $_->{type} eq "command_failed" } @events;
        ok( scalar @failed >= 1, "got events" );
        my $last_failure = $failed[-1];
        my $ok = 1;
        $ok &&= is( $last_failure->{commandName},       "insert", "commandName" );
        $ok &&= ok( defined $last_failure->{requestId}, "requestId" );
        $ok &&= ok( $last_failure->{durationSecs} > 0, "duration" );
        $ok &&= like( $last_failure->{connectionId}, qr/^[^:]+:\d+$/, "connectionId" );
        $ok &&= like( $last_failure->{failure}, qr/fake network error/, "failure msg" );
        $ok &&= isa_ok( $last_failure->{eval_error}, "MongoDB::NetworkError", "eval_error" );
        diag explain $last_failure unless $ok;
    };

    subtest 'find' => sub {
        no warnings 'redefine';
        my $coll = _coll_with_monitor("test");
        $coll->insert_one({}); # force topology discovery
        my $err;
        {
            local *MongoDB::_Link::read = \&_throw_mock_network_error;
            clear_events();
            eval {$coll->find({})->all};
            $err = $@;
        }
        # force reset topology status
        $coll->client->topology_status( refresh => 1 );

        ok( $err, "got exception" );
        my @failed = grep { $_->{type} eq "command_failed" } @events;
        ok( scalar @failed >= 1, "got events" );
        my $last_failure = $failed[-1];
        my $ok = 1;
        $ok &&= is( $last_failure->{commandName},       "find", "commandName" );
        $ok &&= ok( defined $last_failure->{requestId}, "requestId" );
        $ok &&= ok( $last_failure->{durationSecs} > 0, "duration" );
        $ok &&= like( $last_failure->{connectionId}, qr/^[^:]+:\d+$/, "connectionId" );
        $ok &&= like( $last_failure->{failure}, qr/fake network error/, "failure msg" );
        $ok &&= isa_ok( $last_failure->{eval_error}, "MongoDB::NetworkError", "eval_error" );
        diag explain $last_failure unless $ok;
    };
};

sub _coll_with_monitor {
    my $mc     = build_client( monitoring_callback => \&event_cb );
    my $testdb = get_test_db($mc);
    my $col = $testdb->coll(@_);
}

sub _throw_mock_network_error {
    MongoDB::NetworkError->throw("fake network error");
}

sub _test_writes {
    my ($coll) = shift;
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    $coll->insert_one( { x => 1 } );
    $coll->replace_one( { x => 1 }, { x => 0 } );
    $coll->delete_one( { x => 0 } );

    subtest "command_started" => sub {
        my @started = grep { $_->{type} eq "command_started" } @events;

        ok( scalar @started >= 3, "got events" ) or return;

        my $ok = 1;
        for my $cmd (qw/insert update delete/) {
            $ok &&=
            ok( ( scalar grep { $_->{commandName} eq $cmd } @started ), "saw $cmd command" );
        }
        diag explain \@started unless $ok;
    };

    subtest "command_succeeded" => sub {
        my @succeeded = grep { $_->{type} eq "command_succeeded" } @events;

        ok( scalar @succeeded >= 3, "got events" ) or return;

        my $ok = 1;
        for my $cmd (qw/insert update delete/) {
            $ok &&=
            ok( ( scalar grep { $_->{commandName} eq $cmd } @succeeded ), "saw $cmd command" );
        }
        diag explain \@succeeded unless $ok;
    };

    subtest "failed write is still command_succeeded" => sub {
        plan skip_all => "w:0 won't error"
            unless $coll->write_concern->is_acknowledged;
        $coll->insert_one( { _id => 123 } );
        clear_events();
        eval { $coll->insert_one( { _id => 123 } ) };
        ok( $@, "Got exception" );

        my @succeeded = grep { $_->{type} eq "command_succeeded" } @events;
        ok( scalar @succeeded >= 1, "got events" ) or return;
        ok( ( scalar grep { $_->{commandName} eq 'insert' } @succeeded ), "saw insert command" )
            or  diag explain \@succeeded;
    };

}

done_testing;
