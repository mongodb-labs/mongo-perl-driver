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

package MongoDBSpecTest;

use strict;
use warnings;

use Exporter 'import';
use Test::More;
use Path::Tiny;
use JSON::MaybeXS qw( is_bool decode_json );
use MongoDBTest qw(server_type skip_unless_min_version);

our @EXPORT_OK = qw(
    foreach_spec_test
    skip_unless_run_on
    maybe_skip_multiple_mongos
);

sub foreach_spec_test {
    my ($dir, $conn, $callback) = @_;

    $dir = path($dir);
    my $iterator = $dir->iterator( { recurse => 1 } );

    while ( my $path = $iterator->() ) {
        next unless -f $path && $path =~ /\.json$/;

        my $plan = eval { decode_json( $path->slurp_utf8 ) };
        if ($@) {
            die "Error decoding $path: $@";
        }

        my $name = $path->relative($dir)->basename(".json");

        subtest $name => sub {
            skip_unless_run_on($plan->{runOn}, $conn);
            for my $test ( @{ $plan->{tests} } ) {
                subtest $test->{description} => sub {
                    $callback->($test, $plan);
                };
            }
        };
    }
}

sub skip_unless_run_on {
    my ($runon_plan, $conn) = @_;
    return unless $runon_plan;
    my $server_type = server_type($conn);
    my $topology_map = {
        RSPrimary  => 'replicaset',
        Standalone => 'single',
        Mongos     => 'sharded',
    };
    my $topology = $topology_map->{ $server_type } || 'unknown';
    my $topo_version_map = {
        map {
            my $run_on = $_;
            map { $_ => $run_on->{'minServerVersion'} }
                @{ $run_on->{'topology'} || [] }
        } @{ $runon_plan || [] }
    };
    if (keys %$topo_version_map) {
        plan skip_all => sprintf(
            "Test only runs on (%s) topology",
            join('|', keys %$topo_version_map),
        ) unless $topo_version_map->{ $topology };
        my $min_version = $topo_version_map->{ $topology };
        skip_unless_min_version( $conn, $min_version );
    }
}

sub maybe_skip_multiple_mongos {
    my ( $conn, $use_multiple_mongos ) = @_;

    if ( $conn->_topology->type eq 'Sharded' ) {
        if ( $use_multiple_mongos ) {
            plan skip_all => "test deployment must have multiple named mongos"
                if scalar($conn->_topology->all_servers) < 2;
        } else {
            plan skip_all => "test deployment can only use one mongos"
                unless scalar($conn->_topology->all_servers) == 1;
        }
    }
}

1;
