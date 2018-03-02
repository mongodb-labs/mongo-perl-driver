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

use Test::Role::BSONDebug;

Role::Tiny->apply_roles_to_package(
    'MongoDB::BSON', 'Test::Role::BSONDebug',
);

$coll->insert_many( [ map { { wanted => 1, score => $_ } } 0 .. 400 ] );

Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;

subtest 'Shared session in explicit cursor' => sub {

    my $session = $conn->start_session;

    # Cursor passes the session through from the return of result, which is the
    # return of passing the query to send_*_op, which is created in find in
    # ::Collection.
    my $cursor = $coll->find({ wanted => 1 }, { session => $session })->result;

    my $lsid = uuid_to_string( $session->server_session->session_id->{id}->data );

    my $cursor_command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    my $cursor_command_sid = uuid_to_string( $cursor_command->FETCH('lsid')->{id}->data );

    is $cursor_command_sid, $lsid, "Cursor sent with correct lsid";

    my $result_sid = uuid_to_string( $cursor->session->session_id->{id}->data );

    is $result_sid, $lsid, "Query Result contains correct session";

    subtest 'All cursor calls in same session' => sub {
        # Call first batch run outside of loop as doesnt fetch intially
        my @items = $cursor->batch;
        while ( @items = $cursor->batch ) {
            my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;
            ok $command->EXISTS('lsid'), "cursor has session";
            my $cursor_session_id = uuid_to_string( $command->FETCH('lsid')->{id}->data );
            is $cursor_session_id, $lsid, "Cursor is using given session";
        }
    };

    $session->end_session;

    my $retired_session_id = defined $conn->_server_session_pool->_server_session_pool->[0]
        ? uuid_to_string( $conn->_server_session_pool->_server_session_pool->[0]->session_id->{id}->data )
        : '';

    is $retired_session_id, $lsid, "Session returned to pool";

};

Test::Role::BSONDebug::CLEAR_ENCODE_ONE_QUEUE;

subtest 'Shared session in implicit cursor' => sub {

    my $cursor = $coll->find({ wanted => 1 })->result;

    # pull out implicit session
    my $lsid = uuid_to_string( $cursor->session->session_id->{id}->data );

    my $cursor_command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;

    my $cursor_command_sid = uuid_to_string( $cursor_command->FETCH('lsid')->{id}->data );

    is $cursor_command_sid, $lsid, "Cursor sent with correct lsid";

    subtest 'All cursor calls in same session' => sub {
        # Call first batch run outside of loop as doesnt fetch intially
        my @items = $cursor->batch;
        while ( @items = $cursor->batch ) {
            my $command = Test::Role::BSONDebug::GET_LAST_ENCODE_ONE;
            ok $command->EXISTS('lsid'), "cursor has session";
            my $cursor_session_id = uuid_to_string( $command->FETCH('lsid')->{id}->data );
            is $cursor_session_id, $lsid, "Cursor is using given session";
        }
    };

    my $retired_session_id = defined $conn->_server_session_pool->_server_session_pool->[0]
        ? uuid_to_string( $conn->_server_session_pool->_server_session_pool->[0]->session_id->{id}->data )
        : '';

    is $retired_session_id, $lsid, "Session returned to pool at end of cursor";
};

clear_testdbs;

done_testing;
