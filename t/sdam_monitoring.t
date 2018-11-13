#  Copyright 2014 - present MongoDB, Inc.
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
use Test::Deep qw/:v1/;
use JSON::MaybeXS;
use Scalar::Util qw/looks_like_number/;

use MongoDB;
use BSON::Types ':all';

use lib "t/lib";

use MongoDBTest qw/
    build_client
    skip_unless_mongod
/;

skip_unless_mongod();

use MongoDBTest::Callback;

my $cb = MongoDBTest::Callback->new;

my $conn = build_client( monitoring_callback => $cb->callback );
my $topo = $conn->_topology;
$cb->clear_events;
$topo->scan_all_servers;

my @events_cb = @{$cb->events};
my @heartbeat_started_events;
my @heartbeat_succeeded_events;

for my $event_idx ( 0..$#events_cb ) {
    my $event_header = $events_cb[$event_idx]->{'type'} // "";
    if ($event_header eq 'server_heartbeat_started_event') {
        push @heartbeat_started_events, delete $events_cb[$event_idx];
    }
}

for my $event_idx ( 0..$#events_cb ) {
    my $event_header = $events_cb[$event_idx]->{'type'} // "";
    if ($event_header eq 'server_heartbeat_succeeded_event') {
        push @heartbeat_succeeded_events, delete $events_cb[$event_idx];
    }
}

for my $i (0 .. $#heartbeat_succeeded_events) {
    my $heartbeat_succeed = $heartbeat_succeeded_events[$i];
    my $heartbeat_started = $heartbeat_started_events[$i];

    ok( looks_like_number( $heartbeat_succeed->{'duration'} ),
        "duration looks like a number");

    cmp_deeply(
        $heartbeat_succeed,
        {
            connectionId => $heartbeat_started->{'connectionId'},
            duration => ignore(),
            reply => ignore(),
            type => "server_heartbeat_succeeded_event"
        },
        "heartbeat succeed event appears to match the pattern it should"
    );
}

done_testing;
