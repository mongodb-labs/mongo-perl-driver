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
use boolean;

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

subtest 'endSession closes sessions on server' => sub {
    my $session_count = 10;
    my @sessions;
    # for checking later that they've all been culled
    my %session_ids;

    # create all the sessions early so we end up with different ID's for each
    # of them instead of re-using the same session ID for multiple from the
    # pool
    for ( 0 .. $session_count - 1 ) {
        my $session = $conn->start_session;
        $session_ids{ uuid_to_string( $session->server_session->session_id->{id}->data ) } = 1;
        push @sessions, $session;
    }

    for my $i ( 0 .. $session_count - 1 ) {
        $coll->insert_one( { '_id' => $i + 1 }, { session => $sessions[$i] } );
    }

    # Check that all the sessions are actually there on the server
    my $agg_result = $testdb->_aggregate(
        [ { '$listLocalSessions' => {} } ],
    );

    my $s_count = count_sessions_in_hash(
        [ map { $_->{_id} } $agg_result->all ],
        \%session_ids,
    );
    is $s_count, $session_count, 'found all sessions';

    $_->end_session for @sessions;

    $s_count = count_sessions_in_hash (
        [ map { $_->session_id } @{ $conn->_server_session_pool } ],
        \%session_ids,
    );
    is $s_count, $session_count, 'All sessions in pool';

    # called in destruction of client normally
    $conn->_end_all_sessions;

    my $after_end_agg_result = $testdb->_aggregate(
        [ { '$listLocalSessions' => {} } ],
    );

    my $after_end_agg_count = count_sessions_in_hash(
        [ map { $_->{_id} } $after_end_agg_result->all ],
        \%session_ids,
    );

    TODO: {
        local $TODO = "This is basically saying that endSessions isnt working?";
        is $after_end_agg_count, 0, 'All sessions closed';
    }
};

subtest 'expiry of old sessions on retire' => sub {
    my $session_count = 10;
    my @sessions;
    my %session_ids;

    for ( 0 .. $session_count - 1 ) {
        my $session = $conn->start_session;
        $session_ids{ uuid_to_string( $session->server_session->session_id->{id}->data ) } = 1;
        push @sessions, $session;
    }

    is scalar( keys %session_ids ), $session_count, 'got enough unique sessions';

    for my $i ( 0 .. $session_count - 1 ) {
        # force last used to actually be set
        $sessions[$i]->server_session->update_last_use;
        $sessions[$i]->end_session;
    }

    my $before_retire_count = count_sessions_in_hash (
        [ map { $_->session_id } @{ $conn->_server_session_pool } ],
        \%session_ids,
    );
    is $before_retire_count, $session_count, 'All sessions in pool';

    my @to_reorganise;

    # find all sessions to modify
    for my $i ( 0 .. $#{ $conn->_server_session_pool } ) {
        my $uuid = uuid_to_string( $conn->_server_session_pool->[$i]->session_id->{id}->data );
        if ( $session_ids{ $uuid } ) {
            push @to_reorganise, $i;
        }
    }

    # reverse sort array, so that we move the furthest in the list first
    @to_reorganise = sort { $b <=> $a } @to_reorganise;

    # modify and move all known sessions from highest index to lowest
    for my $i ( @to_reorganise ) {
        my $move_sess = splice @{ $conn->_server_session_pool }, $i, 1;
        $move_sess->last_use->subtract( minutes => 40 );
        # send this session to the end of the array
        push @{ $conn->_server_session_pool }, $move_sess;
    }

    my $new_session = $conn->start_session;
    # this should trigger a retiring of sessions from the back of the pool
    $new_session->end_session;

    my $after_retire_count = count_sessions_in_hash (
        [ map { $_->session_id } @{ $conn->_server_session_pool } ],
        \%session_ids,
    );
    is $after_retire_count, 0, 'All sessions retired from pool';

};

sub count_sessions_in_hash {
    my ( $sessions, $session_ids ) = @_;
    my $s_count = 0;
    for my $session ( @$sessions ) {
        my $s_uuid = uuid_to_string ( $session->{id}->data );
        if ( exists $session_ids->{ $s_uuid } ) {
            $s_count++;
        }
    }

    return $s_count;
}

clear_testdbs;

done_testing;
