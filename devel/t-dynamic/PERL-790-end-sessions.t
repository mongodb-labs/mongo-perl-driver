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

use Devel::Dwarn;
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

    is $after_end_agg_count, 0, 'All sessions closed';
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
