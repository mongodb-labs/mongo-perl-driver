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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Try::Tiny;

use MongoDB;
use MongoDB::ReadPreference;
use MongoDB::_Credential;
use MongoDB::_Server;
use MongoDB::_Topology;
use MongoDB::_URI;

subtest "rtt tests" => sub {
    my $iterator = path('t/data/SS/rtt')->iterator( { recurse => 1 } );

    for my $path ( exhaust_sort($iterator) ) {
        next unless -f $path && $path =~ /\.json$/;
        my $plan = eval { decode_json( $path->slurp_utf8 ) };
        if ($@) {
            die "Error decoding $path: $@";
        }
        run_rtt_test( $path->basename(".json"), $plan );
    }

    like(
        exception { create_mock_server( "localhost:27017", -1 ) },
        qr/non-negative number/,
        "negative RTT times throw an excepton"
    );
};

subtest "server selection tests" => sub {
    my $source = path('t/data/SS/server_selection');
    my $iterator = $source->iterator( { recurse => 1 } );

    for my $path ( exhaust_sort($iterator) ) {
        next unless -f $path && $path =~ /\.json$/;
        my $plan = eval { decode_json( $path->slurp_utf8 ) };
        if ($@) {
            die "Error decoding $path: $@";
        }
        run_ss_test( $path->relative($source), $plan );
    }
};

subtest "random selection" => sub {

    my $topo = create_mock_topology( "mongodb://localhost", 'Sharded' );
    $topo->_remove_address("localhost:27017");

    for my $n ( "A" .. "Z" ) {
        my $address = "$n:27017";
        my $server = create_mock_server( $address, 10, type => 'Mongos' );
        $topo->servers->{$server->address} = $server;
        $topo->_update_ewma( $server->address, $server );
    }

    # try up to 20
    my $first = $topo->_find_any_server;

    my $different = 0;
    for ( 1 .. 20 ) {
        my $another = $topo->_find_any_server;
        if ( $first->address ne $another->address ) {
            $different = 1;
            last;
        }
    }

    ok( $different, "servers randomly selected" );
};

sub exhaust_sort {
    my $iter = shift;
    my @result;
    while ( defined( my $i = $iter->() ) ) {
        push @result, $i;
    }
    return sort @result;
}

sub create_mock_topology {
    my ( $uri, $type ) = @_;
    $type ||= 'Single';

    return MongoDB::_Topology->new(
        uri                    => MongoDB::_URI->new( uri => $uri ),
        type                   => $type,
        max_wire_version       => 3,
        min_wire_version       => 0,
        heartbeat_frequency_ms => 3600000,
        last_scan_time => time + 60,
        credential => MongoDB::_Credential->new( mechanism => 'NONE' ),
    );
}

sub create_mock_server {
    my ( $address, $rtt, @args ) = @_;
    return MongoDB::_Server->new(
        address          => $address,
        last_update_time => 0,
        rtt_sec          => $rtt,
        is_master        => { ismaster => 1, ok => 1 },
        @args,
    );
}

sub run_rtt_test {
    my ( $name, $plan ) = @_;

    my $topo = create_mock_topology("mongodb://localhost");

    if ( $plan->{avg_rtt_ms} ne 'NULL' ) {
        $topo->rtt_ewma_sec->{"localhost:27017"} = $plan->{avg_rtt_ms}/1000;
    }

    my $server = create_mock_server( "localhost:2707", $plan->{new_rtt_ms}/1000 );

    $topo->_update_topology_from_server_desc( 'localhost:27017', $server );

    is( $topo->rtt_ewma_sec->{"localhost:27017"}, $plan->{new_avg_rtt}/1000, $name );
}

sub run_ss_test {
    my ( $name, $plan ) = @_;

    $name =~ s{\.json$}{};

    my $topo_desc = $plan->{topology_description};
    my $topo = create_mock_topology( "mongodb://localhost", $topo_desc->{type} );
    $topo->_remove_address("localhost:27017");
    for my $s ( @{ $topo_desc->{servers} } ) {
        my $address = $s->{address};
        my $server  = create_mock_server(
            $address,
            $s->{avg_rtt_ms}/1000,
            type => $s->{type},
            tags => $s->{tags},
        );
        $topo->servers->{$server->address} = $server;
        $topo->_update_ewma( $server->address, $server );
    }

    my $got;
    if ( $plan->{operation} eq 'read' ) {
        my $read_pref = MongoDB::ReadPreference->new(
            mode     => $plan->{read_preference}{mode},
            tag_sets => $plan->{read_preference}{tag_sets},
        );
        my $mode = $read_pref ? lc $read_pref->mode : 'primary';
        my $method =
          $topo->type eq 'Single' || $topo->type eq 'Sharded'
          ? '_find_any_server'
          : "_find_${mode}_server";

        $got = $topo->$method($read_pref);
    }
    else {
        my $method =
          $topo->type eq 'Single' || $topo->type eq 'Sharded'
          ? '_find_any_server'
          : "_find_primary_server";

        $got = $topo->$method;
    }

    if ( my @expect = @{ $plan->{in_latency_window} } ) {
        my $got_address = $got->address;
        my $found = grep { $got_address eq $_->{address} } @expect;
        ok( $found, $name );
    }
    else {
        ok( !defined($got), $name );
    }
}

done_testing;
