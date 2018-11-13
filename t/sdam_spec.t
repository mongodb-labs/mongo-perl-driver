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
use Path::Tiny;
use Time::HiRes qw/time/;

use MongoDB::_Credential;
use MongoDB::_Server;
use MongoDB::_Topology;
use MongoDB::_URI;
use BSON::Types ':all';

use lib "t/lib";

use MongoDBTest::Callback;

my $iterator = path('t/data/SDAM')->iterator({recurse => 1});

my $cb = MongoDBTest::Callback->new;

while ( my $path = $iterator->() ) {
    next unless -f $path && $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ( $@ ) {
        die "Error decoding $path: $@";
    }

    run_test($path->relative('t/data/SDAM'), $plan);
}

sub create_mock_topology {
    my ($name, $string) = @_;

    my $uri = MongoDB::_URI->new( uri => $string );
    my $seed_count = scalar @{ $uri->hostids };

    # XXX this is a hack because the direct tests are written to
    # assume Single, even though this is not implied by the spec
    my $type = ( ($name =~ /^single|monitoring\/standalone/) && $seed_count == 1 )
    ? 'Single' : "Unknown";

    return MongoDB::_Topology->new(
        uri                 => $uri,
        type                => $type,
        replica_set_name    => $uri->options->{replicaset} || '',
        min_server_version  => "0.0.0",
        max_wire_version    => 2,
        min_wire_version    => 0,
        credential          => MongoDB::_Credential->new(
            mechanism => 'NONE',
            monitoring_callback => undef
        ),
        monitoring_callback => $cb->callback,
    );
}

sub run_test {
    my ($name, $plan) = @_;

    $name =~ s/\.json$//;

    # TODO: Fix issue with PossiblePrimary and MongoDB::_Topology::_update_rs_without_primary
    return if $name eq 'rs/primary_hint_from_secondary_with_mismatched_me';

    subtest "$name" => sub {

        my $events_arrayref;
        if ( index($name,"monitoring/") != -1 ) {
            my @events_array = @{ $plan->{'phases'}->[0]->{'outcome'}->{'events'} };

            # Rearranging the test events because server_opening is out of order
            my @server_opening_events;
            for my $event_idx ( 0..$#events_array ) {
                if ((keys %{$events_array[$event_idx]})[0] eq 'server_opening_event') {
                    push @server_opening_events, delete $events_array[$event_idx];
                }
            }

            my @new_events = (map { defined($_) ? ($_) : () } @events_array);

            my $topo_idx = -1;
            for my $event_idx ( 0..$#new_events ) {
                if ((keys %{$new_events[$event_idx]})[0] eq 'topology_description_changed_event') {
                    $topo_idx = $event_idx;
                    last;
                }
            }

            @events_array = (
                @new_events[0..$topo_idx-1],
                @server_opening_events,
                @new_events[$topo_idx..$#new_events],
            );

            $plan->{'phases'}->[0]->{'outcome'}->{'events'} = \@events_array;
            $events_arrayref = \@events_array;
        }


        $cb->clear_events;

        my $topology = create_mock_topology( $name, $plan->{'uri'} );

        for my $phase (@{$plan->{'phases'}}) {

            for my $response (@{$phase->{'responses'}}) {

                my ($addr, $is_master) = @$response;

                if ( defined $is_master->{'electionId'} ){
                    $is_master->{'electionId'} = bson_oid($is_master->{'electionId'}->{'$oid'});
                }

                # Process response
                my $desc = MongoDB::_Server->new(

                    address => $addr,
                    is_master => $is_master,
                    last_update_time => time,
                );

                $topology->_update_topology_from_server_desc( @$response[0], $desc);
            }

            # Need to force this check for compatibility checking
            # scan_all_servers wont work as there arent actually any servers...
            $topology->_check_wire_versions;

            # Check events if monitoring
            if ($phase->{'outcome'}->{'events'}) {
                check_outcome_event(
                    $name,
                    $cb->events,
                    $events_arrayref,
                    $topology)
            }

            # Process outcome
            if ($phase->{'outcome'}->{'servers'}) {
                check_outcome_servers($topology, $phase->{'outcome'}, $name);
            }
        }
    };
}

sub check_outcome_event {
    my ($name, $cb_events, $test_events, $topology) = @_;

    # In these tests we pull out the callback and test events in order to check
    # that they match

    # topology_opening_event test
    # 42 in $test_events topologyId is a dud, check only $cb_events one for existing

    # By using values and brackets around declaration we grab the object so that
    # We can avoid continually calling ->{event}->{thing} due to the nature of
    # the event array given
    my $topo_open_cb = shift @{$cb_events};
    shift @{$test_events};
    is($topo_open_cb->{'topologyId'}, "$topology",
        "topology_open has topologyId that matches topology");

    # server_opening_event test
    # Note from hereon in that we use type and a different structure than the json
    while ( (keys %{$test_events->[0]})[0] eq 'server_opening_event' ) {
        my $server_cb = shift @{$cb_events};
        my ($server_test) = values %{shift @{$test_events}};

        is($server_cb->{'topologyId'}, "$topology",
            "server_opening has topologyId that matches topology");

        is(
            $server_cb->{'address'},
            $server_test->{'address'},
            "server opening matches"
        );
    }

    # topology_description_changed_event test now server(s) have opened
    check_topology_description(
        shift @{$cb_events},
        values %{shift @{$test_events}},
        $topology,
        $name
    );

    # check server_description_changed_event for change in servers
    my $desc_change_cb = shift @{$cb_events};
    my ($desc_change_test) = values %{shift @{$test_events}};

    is($desc_change_cb->{'topologyId'}, "$topology",
        "server_description_changed has topologyId that matches topology");
    is(
        $desc_change_cb->{'address'},
        $desc_change_test->{'address'},
        "server_description_changed server address matches"
    );

    check_server_description(
        $desc_change_cb->{'previousDescription'},
        $desc_change_test->{'previousDescription'}
    );
    check_server_description(
        $desc_change_cb->{'newDescription'},
        $desc_change_test->{'newDescription'}
    );

    # server_closing_event test if any
    while ( (keys %{$test_events->[0]})[0] eq 'server_closed_event') {
        my $server_cb = shift @{$cb_events};
        my ($server_test) = values %{shift @{$test_events}};

        is($server_cb->{'topologyId'}, "$topology",
            "has topologyId that matches topology");

        is(
            $server_cb->{'address'},
            $server_test->{'address'},
            "server closing matches"
        );
    }

    # topology_description_changed_event test now server(s) have been changed
    check_topology_description(
        shift @{$cb_events},
        values %{shift @{$test_events}},
        $topology,
        $name
    );
}

sub check_topology_description {
    my ($topo_desc_cb, $topo_desc_test, $topology, $name) = @_;

    is($topo_desc_cb->{'topologyId'}, "$topology",
        "topology_description_changed has topologyId that matches topology");

    is(
        scalar @{$topo_desc_cb->{'servers'}},
        scalar @{$topo_desc_test->{'servers'}},
        "topology_description_changed correct amount of servers"
    );

    # XXX hack that partially circumvents create_mock_topology hack
    # because we do single too early
    # This only applies to the initial topology opening event
    if (
        (index($name,"monitoring/standalone") != -1) &&
        ($topo_desc_test->{'previousDescription'}->{'topologyType'} eq "Unknown")
    ) {
        $topo_desc_test->{'previousDescription'}->{'topologyType'} = "Single";
    }

    is(
        $topo_desc_cb->{'previousDescription'}->{'topologyType'},
        $topo_desc_test->{'previousDescription'}->{'topologyType'},
        "previous topology description topologyType matches"
    );
    is(
        $topo_desc_cb->{'newDescription'}->{'topologyType'},
        $topo_desc_test->{'newDescription'}->{'topologyType'},
        "new topology description topologyType matches"
    );

    # We insert replica_set_name (setName) on creation, earlier than the spec
    is(
        $topo_desc_cb->{'previousDescription'}->{'setName'},
        $topo_desc_test->{'previousDescription'}->{'setName'},
        "setName matches"
    ) if defined $topo_desc_test->{'previousDescription'}->{'setName'};
    is(
        $topo_desc_cb->{'newDescription'}->{'setName'},
        $topo_desc_test->{'newDescription'}->{'setName'},
        "setName matches"
    ) if defined $topo_desc_test->{'newDescription'}->{'setName'};

    my @sorted_prev_servers = sort {$$a{"address"} cmp $$b{"address"} }
        @{$topo_desc_cb->{'previousDescription'}->{'servers'}};

    for my $i (0..$#{$topo_desc_cb->{'previousDescription'}->{'servers'}}) {
        my $server_cb = $sorted_prev_servers[$i];
        my $server_test = @{$topo_desc_test->{'previousDescription'}->{'servers'}}[$i];

        check_server_description(
            $server_cb,
            $server_test
        );
        check_server_description(
            $server_cb,
            $server_test
        );
    }

    my @sorted_new_servers = sort {$$a{"address"} cmp $$b{"address"} }
        @{$topo_desc_cb->{'newDescription'}->{'servers'}};

    for my $i (0..$#{$topo_desc_cb->{'newDescription'}->{'servers'}}) {
        my $server_cb = $sorted_new_servers[$i];
        my $server_test = @{$topo_desc_test->{'newDescription'}->{'servers'}}[$i];

        check_server_description(
            $server_cb,
            $server_test
        );
        check_server_description(
            $server_cb,
            $server_test
        );
    }
}

sub check_server_description {
    my ($server_cb, $server_test) = @_;

    my $server_cmp = {
        address => $server_test->{'address'},
        arbiters => [],
        hosts => $server_test->{'hosts'},
        passives => [],
        ((exists $server_test->{'primary'}) ?
        (primary => $server_test->{'primary'}) : ()),
        ((exists $server_test->{'setName'}) ?
        (setName => $server_test->{'setName'}) : ()),
        type => $server_test->{'type'}
    };

    cmp_deeply(
        $server_cb,
        superhashof($server_cmp),
        "server matches in single server comparison"
    );
}

sub check_outcome_servers {

    my ($topology, $outcome, $name) = @_;

    my %expected_servers = %{$outcome->{'servers'}};
    my %actual_servers = %{$topology->servers};

    is(
        scalar keys %actual_servers,
        scalar keys %expected_servers,
        'correct amount of servers'
    );

    while (my ($key, $value) = each %expected_servers) {

        if ( ok( (exists $actual_servers{$key}), "$key exists in outcome") ) {
            my $actual_server = $actual_servers{$key};

            is($actual_server->type, $value->{'type'}, 'correct server type');

            my $expected_set_name = defined $value->{'setName'} ? $value->{'setName'} : "";
            is(
                $actual_server->set_name,
                $expected_set_name,
                'correct setName for server'
            );
        }
    }

    my $expected_set_name = defined $outcome->{'setName'} ? $outcome->{'setName'} : "";
    is($topology->replica_set_name, $expected_set_name, 'correct setName for topology');
    is($topology->type, $outcome->{'topologyType'}, 'correct topology type');
    is(
        $topology->logical_session_timeout_minutes, $outcome->{'logicalSessionTimeoutMinutes'},
        'correct ls timeout'
    );
    if ( defined $outcome->{'compatible'} ) {
        my $compatibility = $outcome->{'compatible'} ? 1 : 0;
        # perl driver specifically supports older servers - this goes against
        # spec but allows for support of legacy servers.
        $compatibility = 1 if $name =~ /too_old/;
        is($topology->is_compatible, $compatibility, 'compatibility correct');
    }
}

done_testing;
