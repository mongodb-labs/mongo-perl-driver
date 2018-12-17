#  Copyright 2016 - present MongoDB, Inc.
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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix

use MongoDB;
use MongoDB::ReadPreference;
use MongoDB::_Credential;
use MongoDB::_Server;
use MongoDB::_Topology;
use MongoDB::_URI;

sub exhaust_sort {
    my $iter = shift;
    my @result;
    while ( defined( my $i = $iter->() ) ) {
        push @result, $i;
    }
    return sort @result;
}

sub create_mock_topology {
    my ( $uri, $type, $heartbeat_frequency_ms ) = @_;
    $type ||= 'Single';

    return MongoDB::_Topology->new(
        uri                => MongoDB::_URI->new( uri => $uri ),
        type               => $type,
        min_server_version => "0.0.0",
        max_wire_version   => 3,
        min_wire_version   => 0,
        last_scan_time     => time + 60,
        credential         => MongoDB::_Credential->new(
            mechanism           => 'NONE',
            monitoring_callback => undef
        ),
        (
            defined $heartbeat_frequency_ms
            ? ( heartbeat_frequency_sec => $heartbeat_frequency_ms / 1000 )
            : ()
        ),
        monitoring_callback => undef,
    );
}

my %is_master_tmpl = (
    'RSPrimary'   => { ok => 1, setName => "foo", ismaster  => 1 },
    'RSSecondary' => { ok => 1, setName => "foo", secondary => 1 },
    'Mongos'      => { ok => 1, msg     => 'isdbgrid' },
    'Standalone'  => { ok => 1 },
    'Unknown'     => {},
);

sub create_mock_server {
    my ($s) = @_;

    my %is_master = %{ $is_master_tmpl{ $s->{type} } };
    $is_master{lastWrite}{lastWriteDate} = BSON::Time->new(
        value => $s->{lastWrite}{lastWriteDate}{'$numberLong'}
    ) if exists $s->{lastWrite}{lastWriteDate};
    $is_master{minWireVersion} = 0;
    $is_master{maxWireVersion} = $s->{maxWireVersion} || 0;
    $is_master{tags}           = $s->{tags} if exists $s->{tags};

    return MongoDB::_Server->new(
        address          => $s->{address},
        last_update_time => defined $s->{lastUpdateTime} ? $s->{lastUpdateTime} : 0,
        rtt_sec          => $s->{avg_rtt_ms} ? $s->{avg_rtt_ms} / 1000 : 0,
        is_master        => \%is_master,
    );
}

sub run_staleness_test {
    my ( $name, $plan ) = @_;

    $name =~ s{\.json$}{};

    # prep topology
    my $topo_desc = $plan->{topology_description};
    my $topo      = create_mock_topology( "mongodb://localhost", $topo_desc->{type},
        $plan->{heartbeatFrequencyMS} );
    $topo->_remove_address("localhost:27017");
    for my $s ( @{ $topo_desc->{servers} } ) {
        my $address = $s->{address};
        my $server  = create_mock_server($s);
        $topo->servers->{ $server->address } = $server;
        $topo->_update_ewma( $server->address, $server );
    }
    $topo->_check_wire_versions();

    # select to read
    my $rp        = $plan->{read_preference};
    my $read_pref = eval {
        MongoDB::ReadPreference->new(
            ( exists $rp->{mode}     ? ( mode     => $rp->{mode} )     : () ),
            ( exists $rp->{tag_sets} ? ( tag_sets => $rp->{tag_sets} ) : () ),
            (
                exists $rp->{maxStalenessSeconds}
                ? ( max_staleness_seconds => $rp->{maxStalenessSeconds} )
                : ()
            ),
        );
    };

    # bail out early on RP construction error
    my $rp_err = $@;
    if ($rp_err) {
        ok( $plan->{error}, $name ) or diag "Error: $rp_err";
        return;
    }

    my $mode = $read_pref ? lc $read_pref->mode : 'primary';
    my $method =
        $topo->type eq "Single"  ? '_find_available_server'
      : $topo->type eq "Sharded" ? '_find_readable_mongos_server'
      :                            "_find_${mode}_server";

    my $got = eval { $topo->$method($read_pref) };
    my $err = $@;

    # check errors or suitable list
    if ( $plan->{error} ) {
        ok( length($err), $name )
          or diag "Expected error but got none."
          . ( defined $got ? "Got server " . $got->address : "" );
    }
    elsif ( !@{ $plan->{suitable_servers} } ) {
        ok( !defined($got), $name )
          or diag "Got " . $got->address . " but expected no servers at all.";
    }
    elsif ( !defined($got) ) {
        fail($name);
        diag "Server selection returned no entries";
    }
    else {
        my @expect      = @{ $plan->{suitable_servers} };
        my $got_address = $got->address;
        my $found       = grep { $got_address eq $_->{address} } @expect;
        ok( $found, $name )
          or diag "Got $got_address, expected one of "
          . join( ", ", map { $_->{address} } @expect );
    }
}

my $source = path('t/data/max_staleness');
my $iterator = $source->iterator( { recurse => 1 } );

for my $path ( exhaust_sort($iterator) ) {
    next unless -f $path && $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    my $relpath = $path->relative($source);
    eval { run_staleness_test( $relpath, $plan ) };
    if ( my $err = $@ ) {
        fail("$relpath failed");
        diag($err);
    }
}

# second value undef means error
my @uri_tests = (
    [ "mongodb://host/?readPreference=secondary&maxStalenessSeconds=120", 120 ],
    [ "mongodb://host/?maxStalenessSeconds=120",                          undef ],
    [ "mongodb://host/?readPreference=primary&maxStalenessSeconds=120",   undef ],
    [ "mongodb://host/?readPreference=secondary&maxStalenessSeconds=-1",  -1 ],
    [ "mongodb://host/?readPreference=secondary&maxStalenessSeconds=1",   1 ],
    [ "mongodb://host/?maxStalenessSeconds=-1",                           -1 ],
    [ "mongodb://host/?readPreference=primary&maxStalenessSeconds=-1",    -1 ],
    [ "mongodb://host/?readPreference=secondary&maxStalenessSeconds=0",   undef ],
);

for my $case (@uri_tests) {
    my ( $uri, $value ) = @$case;
    if ($value) {
        my $mc = MongoDB->connect($uri);
        is( $mc->read_preference->max_staleness_seconds, $value, "$uri parsed" );
    }
    else {
        eval { MongoDB->connect($uri) };
        like( $@, qr/(max_staleness_seconds|maxStalenessSeconds)/,
            "$uri is an error" );
    }
}

done_testing;
