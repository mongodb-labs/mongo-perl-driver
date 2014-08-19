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
use Test::More 0.88;
use File::Find;
use Path::Tiny;
use YAML::XS;

use MongoDB;

File::Find::find({wanted => \&wanted, no_chdir => 1}, 't/cluster');

sub wanted {

    if (-f && /^.*\.ya?ml\z/) {

        my $name = path($_)->basename(qr/.ya?ml/);
        my $plan = YAML::XS::LoadFile($_);

        run_test($name, $plan);
    };
}

sub create_mock_cluster {

    my $uri = MongoDB::_URI->new( uri => $_[0] );
    my $type = 'Unknown';
    if (exists $uri->options->{connect}) {
        if ($uri->options->{connect} eq 'replicaSet') {
            $type ='ReplicaSetNoPrimary';
        } elsif ($uri->options->{connect} eq 'direct') {
            $type = 'Single';
        }
    }
    return MongoDB::_Cluster->new( uri => $uri, type => $type );
}

sub run_test {

    my ($test_name, $plan) = @_;

    subtest "test_$test_name" => sub {

        my $cluster = create_mock_cluster( $plan->{'uri'} );

        for my $phase (@{$plan->{'phases'}}) {

            for my $response (@{$phase->{'responses'}}) {

                # Process response
                my $desc = MongoDB::_Server->new(

                    address => @$response[0],
                    is_master => @$response[1],
                    last_update_time => [ Time::HiRes::gettimeofday() ],
                );

                $cluster->_update_cluster_from_server_desc( @$response[0], $desc);
            }

            # Process outcome
            check_outcome($cluster, $phase->{'outcome'});
        }
    };

}

sub check_outcome {

    my ($cluster, $outcome) = @_;

    my %expected_servers = %{$outcome->{'servers'}};
    my %actual_servers = %{$cluster->servers};

    is( scalar keys %actual_servers, scalar keys %expected_servers, 'correct amount of servers');

    while (my ($key, $value) = each %expected_servers) {

        ok( (exists $actual_servers{$key}), "$key exists in outcome");
        my $actual_server = $actual_servers{$key};

        is($actual_server->type, $value->{'type'}, 'correct server type');

        my $expected_set_name = defined $value->{'setName'} ? $value->{'setName'} : "";
        is($actual_server->set_name, $expected_set_name, 'correct setName for server');
    }

    my $expected_set_name = defined $outcome->{'setName'} ? $outcome->{'setName'} : "";
    is($cluster->replica_set_name, $expected_set_name, 'correct setName for cluster');
    is($cluster->type, $outcome->{'clusterType'}, 'correct cluster type');
}

done_testing;
