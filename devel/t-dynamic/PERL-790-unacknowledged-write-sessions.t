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
use UUID::URandom qw/create_uuid/;

use utf8;
use Tie::IxHash;

use MongoDB;
use MongoDB::Error;
use MongoDB::_Types qw/ to_IxHash /;

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
    uuid_to_string
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

plan skip_all => "Requires MongoDB 3.6"
    if $server_version < v3.6.0;

subtest 'Session for ack writes' => sub {

    my $coll = $testdb->get_collection( 'test_collection', { write_concern => { w => 1 } } );

    my $session = $conn->start_session;

    my $result = $coll->insert_one( { _id => 1 }, { session => $session } );
    
    my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    ok $command->EXISTS('lsid'), 'Session found';

    is uuid_to_string( $command->FETCH('lsid')->{id}->data ),
    uuid_to_string( $session->_server_session->session_id->{id}->data ),
    "Session matches";

    my $result2 = $coll->insert_one( { _id => 2 } );

    my $command2 = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    ok $command2->EXISTS('lsid'), 'Implicit session found';
};

subtest 'No session for unac writes' => sub {
    use Test::Role::BSONDebug;

    Role::Tiny->apply_roles_to_package(
        'MongoDB::BSON', 'Test::Role::BSONDebug',
    );

    my $coll = $testdb->get_collection( 'test_collection', { write_concern => { w => 0 } } );

    my $session = $conn->start_session;

    my $result = $coll->insert_one( { _id => 1 }, { session => $session } );
    
    my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    # cannot guarantee ixhash!
    $command = to_IxHash( $command );

    ok ! $command->EXISTS('lsid'), 'No session found';

    my $result2 = $coll->insert_one( { _id => 2 } );

    my $command2 = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    $command2 = to_IxHash( $command2 );

    ok ! $command2->EXISTS('lsid'), 'No implicit session found';
};

clear_testdbs;

done_testing;
