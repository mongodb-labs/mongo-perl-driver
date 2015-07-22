#
#  Copyright 2009-2014 MongoDB, Inc.
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
use JSON::MaybeXS;
use Path::Tiny;
use Try::Tiny;

use MongoDB;
use MongoDB::_Credential;

my $iterator = path('t/data/SDAM')->iterator({recurse => 1});

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
    my $seed_count = scalar @{ $uri->hostpairs };

    # XXX this is a hack because the direct tests are written to
    # assume Single, even though this is not implied by the spec
    my $type = ( $name =~ /^single/ && $seed_count == 1 ) ? 'Single' : "Unknown";

    return MongoDB::_Topology->new(
        uri => $uri,
        type => $type,
        replica_set_name => $uri->options->{replicaset} || '',
        max_wire_version => 2,
        min_wire_version => 0,
        credential => MongoDB::_Credential->new( mechanism => 'NONE' ),
    );
}

sub run_test {

    my ($name, $plan) = @_;

    $name =~ s/\.json$//;

    subtest "$name" => sub {

        my $topology = create_mock_topology( $name, $plan->{'uri'} );

        for my $phase (@{$plan->{'phases'}}) {

            for my $response (@{$phase->{'responses'}}) {

                my ($addr, $is_master) = @$response;
                $is_master->{me} = $addr
                    if $is_master->{setName} && ! exists $is_master->{me};

                # Process response
                my $desc = MongoDB::_Server->new(

                    address => $addr,
                    is_master => $is_master,
                    last_update_time => [ Time::HiRes::gettimeofday() ],
                );

                $topology->_update_topology_from_server_desc( @$response[0], $desc);
            }

            # Process outcome
            check_outcome($topology, $phase->{'outcome'});
        }
    };

}

sub check_outcome {

    my ($topology, $outcome, $start_type) = @_;

    my %expected_servers = %{$outcome->{'servers'}};
    my %actual_servers = %{$topology->servers};

    is( scalar keys %actual_servers, scalar keys %expected_servers, 'correct amount of servers');

    while (my ($key, $value) = each %expected_servers) {

        if ( ok( (exists $actual_servers{$key}), "$key exists in outcome") ) {
            my $actual_server = $actual_servers{$key};

            is($actual_server->type, $value->{'type'}, 'correct server type');

            my $expected_set_name = defined $value->{'setName'} ? $value->{'setName'} : "";
            is($actual_server->set_name, $expected_set_name, 'correct setName for server');
        }
    }

    my $expected_set_name = defined $outcome->{'setName'} ? $outcome->{'setName'} : "";
    is($topology->replica_set_name, $expected_set_name, 'correct setName for topology');
    is($topology->type, $outcome->{'topologyType'}, 'correct topology type');
}

done_testing;
