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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator; 

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
/;

# This test starts servers on localhost ports 27017, 27018 and 27019. We skip if
# these aren't available.

my $orc =
MongoDBTest::Orchestrator->new(
  config_file => "devel/config/replicaset-single-3.6.yml" );
$orc->start;

$ENV{MONGOD} = $orc->as_uri;

print $ENV{MONGOD};

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

plan skip_all => "Requires MongoDB 3.6"
    if $server_version < v3.6.0;

# Last in First out
subtest 'LIFO Pool' => sub {
    my $session_a = $conn->start_session;
    my $session_b = $conn->start_session;

    # cache the ID's
    my $id_a = $session_a->session_id;
    my $id_b = $session_b->session_id;
    ok defined $id_a->{id}, 'Session A ID defined';
    ok defined $id_b->{id}, 'Session B ID defined';

    $session_a->end_session;
    $session_b->end_session;

    # Internal only check, not part of spec
    is $session_a->server_session, undef, 'Session A has been returned';
    is $session_b->server_session, undef, 'Session B has been returned';

    my $session_c = $conn->start_session;
    ok defined $session_c->session_id->{id}, 'Session C ID defined';
    is $session_c->session_id->{id}, $id_b->{id}, 'Session C same ID as Session B';

    my $session_d = $conn->start_session;
    ok defined $session_d->session_id->{id}, 'Session D ID defined';
    is $session_d->session_id->{id}, $id_a->{id}, 'Session D same ID as Session A';
};

use Devel::Dwarn;
subtest 'clusterTime in commands' => sub {

    use Test::Role::BSONDebug;

    Role::Tiny->apply_roles_to_package(
        'MongoDB::BSON', 'Test::Role::BSONDebug',
    );

    subtest 'ping' => sub {
        my $local_client = get_high_heartbeat_client();

        my $ping_result = $local_client->send_admin_command(Tie::IxHash->new('ping' => 1));

        my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;
        my $result = Test::Role::BSONDebug::GET_LAST_DECODE_ONE;

        ok $command->EXISTS('ping'), 'ping in sent command';

        if ( $local_client->_topology->wire_version_ceil >= 6 ) {
            ok $command->EXISTS('$clusterTime'), 'clusterTime in sent command';

            my $ping_result2 = $local_client->send_admin_command(Tie::IxHash->new('ping' => 1));

            my $command2 = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

            is $command2->FETCH('$clusterTime')->{clusterTime}->{sec},
               $result->{'$clusterTime'}->{clusterTime}->{sec},
               "clusterTime matches";
        }
    };

    Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
    Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;

    subtest 'aggregate' => sub {
        my $local_client = get_high_heartbeat_client();
        my $local_db = get_test_db($local_client);
        my $local_coll = get_unique_collection($local_db, 'cluster_agg');

        $local_coll->insert_many( [ { wanted => 1, score => 56 },
                              { wanted => 1, score => 72 },
                              { wanted => 1, score => 96 },
                              { wanted => 1, score => 32 },
                              { wanted => 1, score => 61 },
                              { wanted => 1, score => 33 },
                              { wanted => 0, score => 1000 } ] );

        my $agg_result = $local_coll->aggregate( [
            { '$match'   => { wanted => 1 } },
            { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } }
        ] );

        my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;
        my $result = Test::Role::BSONDebug::GET_LAST_DECODE_ONE;

        ok $command->EXISTS('aggregate'), 'aggregate in sent command';

        if ( $local_client->_topology->wire_version_ceil >= 6 ) {
            ok $command->EXISTS('$clusterTime'), 'clusterTime in sent command';

            my $agg_result2 = $local_coll->aggregate( [ { '$match'   => { wanted => 1 } },
                { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } } ] );

            my $command2 = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

            is $command2->FETCH('$clusterTime')->{clusterTime}->{sec},
               $result->{'$clusterTime'}->{clusterTime}->{sec},
               "clusterTime matches";
        }
    };

    Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
    Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;

    subtest 'find' => sub {
        my $local_client = get_high_heartbeat_client();
        my $local_db = get_test_db($local_client);
        my $local_coll = get_unique_collection($local_db, 'cluster_find');

        $local_coll->insert_one({_id => 1});

        # need to actually call ->result to make it touch the database, and
        # explain 1 to get it to show the whole returned result
        my $find_result = $local_coll->find({_id => 1})->result;

        my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;
        my $result = Test::Role::BSONDebug::GET_LAST_DECODE_ONE;

        ok $command->EXISTS('find'), 'find in sent command';

        if ( $local_client->_topology->wire_version_ceil >= 6 ) {
            ok $command->EXISTS('$clusterTime'), 'clusterTime in sent command';

            my $find_result2 = $local_coll->find({_id => 1})->result;

            my $command2 = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

            is $command2->FETCH('$clusterTime')->{clusterTime}->{sec},
               $result->{'$clusterTime'}->{clusterTime}->{sec},
               "clusterTime matches";
        }
    };

    Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
    Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;

    subtest 'insert_one' => sub {
        my $local_client = get_high_heartbeat_client();
        my $local_db = get_test_db($local_client);
        my $local_coll = get_unique_collection($local_db, 'cluster_find');

        my $insert_result = $local_coll->insert_one({_id => 1});

        my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;
        my $result = Test::Role::BSONDebug::GET_LAST_DECODE_ONE;

        ok $command->EXISTS('insert'), 'insert in sent command';

        if ( $local_client->_topology->wire_version_ceil >= 6 ) {
            ok $command->EXISTS('$clusterTime'), 'clusterTime in sent command';

            my $insert_result2 = $local_coll->insert_one({_id => 2});

            my $command2 = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

            is $command2->FETCH('$clusterTime')->{clusterTime}->{sec},
               $result->{'$clusterTime'}->{clusterTime}->{sec},
               "clusterTime matches";
        }
    };

    Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
    Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;
};

sub get_high_heartbeat_client {
    my $local_client = build_client(
        # You want big number? we give you big number
        heartbeat_frequency_ms => 9_000_000_000,
    );

    # Make sure we have clusterTime already populated
    $local_client->send_admin_command(Tie::IxHash->new('ismaster' => 1));

    return $local_client;
}

clear_testdbs;

done_testing;
