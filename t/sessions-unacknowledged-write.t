#  Copyright 2018 - present MongoDB, Inc.
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

plan skip_all => "Requires MongoDB 3.6"
    if $server_version < v3.6.0;

plan skip_all => "Sessions unsupported on standalone server"
    if $server_type eq 'Standalone';

plan skip_all => "deployment does not support sessions"
    unless $conn->_topology->_supports_sessions;

subtest 'Session for ack writes' => sub {

    my $coll = $testdb->get_collection( 'test_collection', { write_concern => { w => 1 } } );

    my $session = $conn->start_session;

    my $result = $coll->insert_one( { _id => 1 }, { session => $session } );
    
    my $command = $events[-2]->{ command };

    ok exists $command->{'lsid'}, 'Session found';

    is uuid_to_string( $command->{'lsid'}->{id}->data ),
    uuid_to_string( $session->_server_session->session_id->{id}->data ),
    "Session matches";

    my $result2 = $coll->insert_one( { _id => 2 } );

    my $command2 = $events[-2]->{ command };

    ok $command2->{'lsid'}, 'Implicit session found';
};

subtest 'No session for unac writes' => sub {

    my $coll = $testdb->get_collection( 'test_collection', { write_concern => { w => 0 } } );

    my $session = $conn->start_session;

    my $result = $coll->insert_one( { _id => 1 }, { session => $session } );
    
    my $command = $events[-2]->{ command };

    ok ! exists $command->{'lsid'}, 'No session found';

    my $result2 = $coll->insert_one( { _id => 2 } );

    my $command2 = $events[-2]->{ command };

    ok ! exists $command2->{'lsid'}, 'No implicit session found';
};

clear_testdbs;

done_testing;
