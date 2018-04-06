#
#  Copyright 2009-2013 MongoDB, Inc.
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

# Test in t-dynamic as not sure if failover should be tested on install?

use strict;
use warnings;
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;
use boolean;

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    clear_testdbs
    get_unique_collection
    server_version
    server_type
    check_min_server_version
    get_features
    skip_unless_mongod
/;

skip_unless_mongod();

my @events;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }

my $conn = build_client(
    retry_writes => 1,
    monitoring_callback => \&event_cb,
);
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $features       = get_features($conn);

plan skip_all => "retryableWrites not supported on this MongoDB"
    unless ( $features->supports_retryWrites );

sub check_event_no_txn {
    my $cmd = shift;
    my $op = shift;
    is $events[-2]->{ commandName }, $cmd, "$op command correct";
    is $events[-2]->{ type }, 'command_started', "$op command started";
    ok ! exists $events[-2]->{ command }->{ txnNumber }, "$op no transaction number";
};

subtest 'unacknowledged writes no transaction' => sub {
    my $coll = get_unique_collection( $testdb, 'cmd_con_792_unac', { write_concern => { w => 0 } } );

    clear_events();
    $coll->insert_one( { _id => 1 } );
    check_event_no_txn( 'insert', 'insert_one' );

    clear_events();
    $coll->insert_many( [
        { _id => 2 },
        { _id => 3 },
        { _id => 4 },
        { _id => 5 },
        { _id => 6 },
        { _id => 7 },
        { _id => 8 },
    ] );
    check_event_no_txn( 'insert', 'insert_many' );

    clear_events();
    $coll->replace_one(
        { _id => 1 },
        { _id => 1, foo => 'bar' }
    );
    check_event_no_txn( 'update', 'replace_one' );

    clear_events();
    $coll->update_one(
        { _id => 1 },
        { '$set' => { foo => 'qux' } },
    );
    check_event_no_txn( 'update', 'update_one' );

    clear_events();
    $coll->update_many(
        { _id => { '$in' => [1,2,3] } },
        { '$set' => { foo => 'qux' } },
    );
    check_event_no_txn( 'update', 'update_many' );

    clear_events();
    $coll->delete_one(
        { _id => 1 },
    );
    check_event_no_txn( 'delete', 'delete_one' );

    clear_events();
    $coll->delete_many(
        { _id => { '$in' => [2,3] } },
    );
    check_event_no_txn( 'delete', 'delete_many' );

    clear_events();
    $coll->find_one_and_delete(
        { _id => 4 },
    );
    check_event_no_txn( 'findAndModify', 'find_one_and_delete' );

    clear_events();
    $coll->find_one_and_replace(
        { _id => 5 },
        { _id => 5, flibble => 'bee' },
    );
    check_event_no_txn( 'findAndModify', 'find_one_and_replace' );

    clear_events();
    $coll->find_one_and_update(
        { _id => 6 },
        { '$set' => { bar => 'baz' } },
    );
    check_event_no_txn( 'findAndModify', 'find_one_and_update' );

    # building an (un)ordered bulk is the same as using bulkWrite
    clear_events();
    $coll->bulk_write( [
        insert_one => [ { _id => 1 } ],
        insert_one => [ { _id => 2 } ],
    ] );
    check_event_no_txn( 'insert', 'bulk_write ordered' );

    clear_events();
    $coll->bulk_write( [
        insert_one => [ { _id => 1 } ],
        insert_one => [ { _id => 2 } ],
    ], { ordered => 0 } );
    check_event_no_txn( 'insert', 'bulk_write unordered' );
};


subtest 'unsupported single statement writes' => sub {
    my $coll = get_unique_collection( $testdb, 'cmd_con_792_unsup' );

    $coll->insert_many( [
        { _id => 1 },
        { _id => 2 },
        { _id => 3 },
    ] );

    clear_events();
    $coll->update_many(
        { _id => { '$in' => [1,2,3] } },
        { '$set' => { foo => 'qux' } },
    );
    check_event_no_txn( 'update', 'update_many' );

    clear_events();
    $coll->delete_many(
        { _id => { '$in' => [2,3] } },
    );
    check_event_no_txn( 'delete', 'delete_many' );
};

subtest 'unsupported multi statement writes' => sub {
    my $coll = get_unique_collection( $testdb, 'cmd_con_792_u_multi' );

    $coll->insert_many( [
        { _id => 1 },
        { _id => 2 },
        { _id => 3 },
    ] );

    clear_events();
    $coll->bulk_write( [
        update_many => [
          { _id => { '$in' => [1,2,3] } },
          { '$set' => { foo => 'qux' } },
        ],
    ] );
    check_event_no_txn( 'update', 'bulk_write update_many' );

    clear_events();
    $coll->bulk_write( [
        delete_many => [
          { _id => { '$in' => [1,2,3] } },
        ],
    ] );
    check_event_no_txn( 'delete', 'bulk_write delete_many' );
};

subtest 'unsupported write commands' => sub {
    my $coll = get_unique_collection( $testdb, 'cmd_con_792_u_write' );

    $coll->insert_many( [
        map { { count => $_ } } 1..20
    ] );

    clear_events();
    my $result = $coll->aggregate( [
        { '$match' => { count => { '$gt' => 0 } } },
        { '$out' => 'test_out' }
    ] );
    check_event_no_txn( 'aggregate', 'aggregate with $out' );
};

sub check_event_with_txn {
    my $cmd = shift;
    my $op = shift;
    is $events[-2]->{ commandName }, $cmd, "$op command correct";
    is $events[-2]->{ type }, 'command_started', "$op command started";
    isa_ok $events[-2]->{ command }->{ txnNumber }, "Math::BigInt", "$op has transaction number";
}

subtest 'supported single statement writes' => sub {
    my $coll = get_unique_collection( $testdb, 'cmd_con_792_sup' );

    $coll->insert_many( [
        { _id => 2 },
        { _id => 3 },
        { _id => 4 },
        { _id => 5 },
        { _id => 6 },
        { _id => 7 },
        { _id => 8 },
    ] );

    clear_events();
    $coll->insert_one( { _id => 1 } );
    check_event_with_txn( 'insert', 'insert_one' );

    clear_events();
    $coll->replace_one(
        { _id => 1 },
        { _id => 1, foo => 'bar' }
    );
    check_event_with_txn( 'update', 'replace_one' );

    clear_events();
    $coll->update_one(
        { _id => 1 },
        { '$set' => { foo => 'qux' } },
    );
    check_event_with_txn( 'update', 'update_one' );

    clear_events();
    $coll->delete_one(
        { _id => 1 },
    );
    check_event_with_txn( 'delete', 'delete_one' );

    clear_events();
    $coll->find_one_and_delete(
        { _id => 4 },
    );
    check_event_with_txn( 'findAndModify', 'find_one_and_delete' );

    clear_events();
    $coll->find_one_and_replace(
        { _id => 5 },
        { _id => 5, flibble => 'bee' },
    );
    check_event_with_txn( 'findAndModify', 'find_one_and_replace' );

    clear_events();
    $coll->find_one_and_update(
        { _id => 6 },
        { '$set' => { bar => 'baz' } },
    );
    check_event_with_txn( 'findAndModify', 'find_one_and_update' );
};

subtest 'supported multi statement writes' => sub {
    my $coll = get_unique_collection( $testdb, 'cmd_con_792_s_multi' );

    clear_events();
    $coll->insert_many( [
        map { { _id => $_ } } 1..5
    ], { ordered => 1 } );
    check_event_with_txn( 'insert', 'insert_many' );

    clear_events();
    $coll->insert_many( [
        map { { _id => $_ } } 6..10
    ], { ordered => 0 } );
    check_event_with_txn( 'insert', 'insert_many' );

    # building an (un)ordered bulk is the same as using bulkWrite
    clear_events();
    $coll->bulk_write( [
        insert_one => [ { _id => 11 } ],
        insert_one => [ { _id => 12 } ],
    ] );
    check_event_with_txn( 'insert', 'bulk_write ordered' );

    clear_events();
    $coll->bulk_write( [
        insert_one => [ { _id => 13 } ],
        insert_one => [ { _id => 14 } ],
    ], { ordered => 0 } );
    check_event_with_txn( 'insert', 'bulk_write unordered' );
};

done_testing;
