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
use UUID::Tiny ':std'; # Use newer interface

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

use Test::Role::BSONDebug;
Role::Tiny->apply_roles_to_package(
    'MongoDB::BSON', 'Test::Role::BSONDebug',
);

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

Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;

subtest 'session operation_time undef on init' => sub {
    my $session = $conn->start_session;
    is $session->operation_time, undef, 'is undef';
};

Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;

subtest 'first read' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    my $response = $coll->find_one({ _id => 1 }, {}, { session => $session });

    my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    ok ! $command->EXISTS( 'afterClusterTime' ), 'afterClusterTime not sent on first read';
};

Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;
Test::Role::BSONDebug::CLEAR_DECODE_ONE_QUEUE;

subtest 'update operation_time' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    is $session->operation_time, undef, 'Empty operation time';

    my $response = $coll->insert_one({ _id => 1 }, { session => $session });

    my $bson = Test::Role::BSONDebug::GET_LAST_DECODE_ONE;

    is $session->operation_time, $bson->{operation_time}, 'response has operation time and is updated in session';

    $session->end_session;

    $session = $conn->start_session({ causalConsistency => 1 });

    my $err = exception { $coll->insert_one({ _id => 1 }, { session => $session }) };

    isa_ok( $err, 'MongoDB::DatabaseError', "duplicate insert error" );

    my $error_bson = Test::Role::BSONDebug::GET_LAST_DECODE_ONE;

    is $session->operation_time, $error_bson->{operation_time}, 'response has operation time and is updated in session';
};

clear_testdbs;

done_testing;
