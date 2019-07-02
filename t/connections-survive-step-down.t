#  Copyright 2009 - present MongoDB, Inc.
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
use Test::More;
use Test::Fatal;
use Storable qw( dclone );
use Safe::Isa;

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
/;

skip_unless_mongod();
skip_unless_failpoints_available();

my $main_conn      = build_client();
my $server_version = server_version($main_conn);
my $server_type    = server_type($main_conn);

plan skip_all => 'MongoDB version 4.0 or higher required'
  unless $server_version >= version->parse('v4.0.0');
plan skip_all => 'Require replica set'
    unless $server_type eq 'RSPrimary';

ok( $main_conn->connected, "client is connected" );
isa_ok( $main_conn, 'MongoDB::MongoClient' );

my @events;

sub clear_events { @events = () }

sub event_cb { push @events, dclone $_[0] }

sub connections_created {
    my $server_status = shift->send_admin_command([serverStatus => 1]);
    return $server_status->{output}{connections}{totalCreated};
}

sub build_insert_failpoint {
    my $err_code = shift;
    return {
        configureFailPoint => "failCommand",
        mode => {
            times => 1
        },
        data => {
            failCommands => ["insert"],
            errorCode => $err_code,
        }
    };
}

sub test_reset_conn_pool {
    my ($conn, $coll, $err_code) = @_;
    my $init_total_created = connections_created($conn);
    my $failpoint = build_insert_failpoint($err_code);
    set_failpoint($conn, $failpoint);
    clear_events();
    eval { $coll->insert_one({ test => 1}) };
    ok(my $err = $@, 'got failCommand error');
    ok($err->$_isa('MongoDB::DatabaseError'), 'failCommand err');
    is($err->code, $err_code, 'check error code');
    is(connections_created($conn), $init_total_created + 1,
       'new connections in');
    clear_failpoint($conn, $failpoint);
}

my $tests = {
    '1-getMore Iteration' => sub {
        my ($conn, $db, $coll) = @_;
        plan skip_all => 'MongoDB version 4.2 or higher required'
            unless $server_version >= version->parse('v4.2.0');
        my $init_total_created = connections_created($conn);
        $coll->insert_many([
            { x => 1 },
            { x => 2 },
            { x => 3 },
            { x => 4 },
            { x => 5 },
        ]);
        ok(my $res = $coll->find( { x => { '$gt' => 3 } }, { batchSize => 2 } ),
           'find');
        ok($res->next, 'first batch retrieve');
        my $repl = $conn->send_admin_command([ replSetStepDown => 5, force => 1 ]);
        ok($repl->{output}{ok}, 'repl ok');
        my @got_events = grep { $_->{commandName} eq 'replSetStepDown' } @events;
        ok(@got_events == 2, 'repl command');
        is($got_events[1]->{type}, 'command_succeeded', 'succeeded');
        ok(my $doc = $res->next, 'next batch retrieve');
        is(connections_created($conn), $init_total_created,
            'no new connections');
    },
    '2-Not Master - Keep Connection Pool' => sub {
        my ($conn, $db, $coll) = @_;
        plan skip_all => 'MongoDB version 4.2 or higher required'
            unless $server_version >= version->parse('v4.2.0');
        my $init_total_created = connections_created($conn);
        my $err_code = 10107;
        my $failpoint = build_insert_failpoint($err_code);
        set_failpoint($conn, $failpoint);
        clear_events();
        eval { $coll->insert_one({ test => 1}) };
        ok(my $err = $@, 'got failCommand error');
        ok($err->$_isa('MongoDB::DatabaseError'), 'failCommand err');
        is($err->code, $err_code, 'check error code');
        $coll->insert_one({ test => 1});
        is($events[-1]->{type}, 'command_succeeded', 'insert succeeded');
        is(connections_created($conn), $init_total_created,
            'no new connections');
        clear_failpoint($conn, $failpoint);
    },
    '3-Not Master - Reset Connection Pool' => sub {
        my ($conn, $db, $coll) = @_;
        plan skip_all => 'MongoDB version 4.0 maximum required'
          unless $server_version < version->parse('v4.1.0');
        my $err_code = 10107;
        test_reset_conn_pool($conn, $coll, $err_code);
    },
    '4-Shutdown in progress - Reset Connection Pool' => sub {
        my ($conn, $db, $coll) = @_;
        my $err_code = 91;
        test_reset_conn_pool($conn, $coll, $err_code);
    },
    '5-Interrupted at shutdown - Reset Connection Pool' => sub {
        my ($conn, $db, $coll) = @_;
        my $err_code = 11600;
        test_reset_conn_pool($conn, $coll, $err_code);
    },
};
foreach my $test_desc (sort keys %$tests) {
    my $conn = build_client(
        monitoring_callback => \&event_cb,
        retry_writes => 0,
    );

    my ($db_name, $coll_name) = ('step-down', 'step-down');
    my $db = $conn->get_database($db_name);
    isa_ok( $db, 'MongoDB::Database', 'get_database' );
    my $coll = $db->get_collection(
        'step-down',
        { write_concern => { w => 'majority', wtimeout => 10000 } }
    );
    alarm 20;
    CREATE_COLL : while (1) {
        eval {
            $coll->drop;
            $db->run_command([ create => $coll_name ]);
        };
        my $err = $@;
        last CREATE_COLL unless $err && $err->$_isa('MongoDB::SelectionError');
    }
    alarm 0;

    my $test_sub = $tests->{$test_desc};
    clear_events();
    subtest $test_desc => sub {
        $test_sub->($conn, $db, $coll);
    };
}

done_testing;
