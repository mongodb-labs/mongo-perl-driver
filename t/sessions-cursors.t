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

use lib "t/lib";

use MongoDBTest qw/
    build_client
    skip_unless_mongod
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
    uuid_to_string
/;

skip_unless_mongod();

my @events;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }

my $conn           = build_client(
    monitoring_callback => \&event_cb,
);
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

plan skip_all => "Requires MongoDB 3.6"
    if $server_version < v3.6.0;

plan skip_all => "Sessions unsupported on standalone server"
    if $server_type eq 'Standalone';

plan skip_all => "deployment does not support sessions"
    unless $conn->_topology->_supports_sessions;

$coll->insert_many( [ map { { wanted => 1, score => $_ } } 0 .. 400 ] );

clear_events();

subtest 'Shared session in explicit cursor' => sub {

    my $session = $conn->start_session;

    # Cursor passes the session through from the return of result, which is the
    # return of passing the query to send_*_op, which is created in find in
    # ::Collection.
    my $cursor = $coll->find({ wanted => 1 }, { batchSize => 100, session => $session })->result;

    my $lsid = uuid_to_string( $session->_server_session->session_id->{id}->data );

    my $cursor_command = $events[-2]->{ command };

    my $cursor_command_sid = uuid_to_string( $cursor_command->{'lsid'}->{id}->data );

    is $cursor_command_sid, $lsid, "Cursor sent with correct lsid";

    my $result_sid = uuid_to_string( $cursor->_session->session_id->{id}->data );

    is $result_sid, $lsid, "Query Result contains correct session";

    subtest 'All cursor calls in same session' => sub {
        # Call first batch run outside of loop as doesnt fetch intially
        my @items = $cursor->batch;
        while ( @items = $cursor->batch ) {
            my $command = $events[-2]->{ command };
            ok exists $command->{'lsid'}, "cursor has session";
            my $cursor_session_id = uuid_to_string( $command->{'lsid'}->{id}->data );
            is $cursor_session_id, $lsid, "Cursor is using given session";
        }
    };

    $session->end_session;

    my $retired_session_id = defined $conn->_server_session_pool->_server_session_pool->[0]
        ? uuid_to_string( $conn->_server_session_pool->_server_session_pool->[0]->session_id->{id}->data )
        : '';

    is $retired_session_id, $lsid, "Session returned to pool";

};

clear_events();

subtest 'Shared session in implicit cursor' => sub {

    my $cursor = $coll->find({ wanted => 1 })->result;

    # pull out implicit session
    my $lsid = uuid_to_string( $cursor->_session->session_id->{id}->data );

    my $cursor_command = $events[-2]->{ command };

    my $cursor_command_sid = uuid_to_string( $cursor_command->{'lsid'}->{id}->data );

    is $cursor_command_sid, $lsid, "Cursor sent with correct lsid";

    subtest 'All cursor calls in same session' => sub {
        # Call first batch run outside of loop as doesnt fetch intially
        my @items = $cursor->batch;
        while ( @items = $cursor->batch ) {
            my $command = $events[-2]->{ command };
            ok exists $command->{'lsid'}, "cursor has session";
            my $cursor_session_id = uuid_to_string( $command->{'lsid'}->{id}->data );
            is $cursor_session_id, $lsid, "Cursor is using given session";
        }
    };

    # implicit session goes out of scope when cursor does
    undef $cursor;

    my $retired_session_id = defined $conn->_server_session_pool->_server_session_pool->[0]
        ? uuid_to_string( $conn->_server_session_pool->_server_session_pool->[0]->session_id->{id}->data )
        : '';

    is $retired_session_id, $lsid, "Session returned to pool at end of cursor lifetime";
};

clear_testdbs;

done_testing;
