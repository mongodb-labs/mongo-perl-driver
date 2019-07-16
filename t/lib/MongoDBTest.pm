#  Copyright 2013 - present MongoDB, Inc.
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

package MongoDBTest;

use strict;
use warnings;

use Exporter 'import';
use MongoDB;
use BSON;
use Test::More;
use Path::Tiny;
use boolean;
use version;

our @EXPORT_OK = qw(
  build_client
  get_test_db
  server_version
  server_type
  clear_testdbs
  get_capped
  skip_if_mongod
  skip_unless_mongod
  skip_unless_failpoints_available
  set_failpoint
  clear_failpoint
  skip_unless_sessions
  skip_unless_transactions
  to_snake_case
  remap_hashref_to_snake_case
  uri_escape
  get_unique_collection
  get_features
  check_min_server_version
  uuid_to_string
  skip_unless_min_version
);

my @testdbs;

sub _check_local_rs {

}

# abstract building a connection
sub build_client {
    my %args = @_;
    my $host =
        exists $args{host}  ? delete $args{host}
      : exists $ENV{MONGOD} ? $ENV{MONGOD}
      :                       'localhost';

    my $ssl;
    if ( $ENV{EVG_ORCH_TEST} && $ENV{SSL} eq 'ssl' ) {
        $ssl = {
            SSL_cert_file => $ENV{EVG_TEST_SSL_PEM_FILE},
            SSL_ca_file   => $ENV{EVG_TEST_SSL_CA_FILE},
        };
    }

    my $codec;
    if ( $ENV{PERL_MONGO_TEST_CODEC_WRAPPED} ) {
        $codec = BSON->new(
            ordered      => 1,
            wrap_dbrefs  => 1,
            wrap_numbers => 1,
            wrap_strings => 1,
        );
    }

    # allow whole test suite to be run with compression enabled
    if ( my $comp = $ENV{PERL_MONGO_TEST_COMPRESSION} ) {
        $args{compressors} ||= [$comp];
    }

    # long query timeout may help spurious failures on heavily loaded CI machines
    return MongoDB->connect(
        $host,
        {
            ssl                         => $ssl || $ENV{MONGO_SSL},
            socket_timeout_ms           => 120000,
            server_selection_timeout_ms => $ENV{ATLAS_PROXY} ? 10000 : 2000,
            server_selection_try_once   => 0,
            wtimeout                    => 15000,
            compressors                 => ['zlib'],
            retry_writes                => 1,
            ( $codec ? ( bson_codec => $codec ) : () ),
            %args,
        }
    );
}

sub get_test_db {
    my $conn   = shift;
    my $prefix = shift || 'testdb';
    my $testdb = $prefix . int( rand( 2**31 ) );
    my $db     = $conn->get_database($testdb) or die "Can't get database\n";
    push( @testdbs, $db );
    return $db;
}

sub get_unique_collection {
    my ( $db, $prefix, $options ) = @_;
    return $db->get_collection(
        sprintf( '%s_%d_%d', $prefix, time(), int( rand(999999) ) ), $options, );
}

sub get_capped {
    my ( $db, $name, %args ) = @_;
    $name ||= 'capped' . int( rand( 2**31 ) );
    $args{size} ||= 500_000;
    $db->run_command( [ create => $name, capped => true, %args ] );
    return $db->get_collection($name);
}

sub skip_unless_mongod {
    eval {
        my $conn = build_client( server_selection_timeout_ms => 10000 );
        my $topo = $conn->_topology;
        $topo->scan_all_servers;
        my $link;
        eval { $link = $topo->get_writable_link }
          or die "couldn't connect: $@";
        $conn->get_database("admin")->run_command( { serverStatus => 1 } )
          or die "Database has auth enabled\n";
        my $server = $link->server;
        if ( !$ENV{MONGOD} && $topo->type eq 'Single' && $server->type =~ /^RS/ ) {
            # direct connection to RS member on default, so add set name
            # via MONGOD environment variable for subsequent use
            $ENV{MONGOD} = "mongodb://localhost/?replicaSet=" . $server->set_name;
        }
##        $conn->_topology->_dump;
    };

    if ($@) {
        ( my $err = $@ ) =~ s/\n//g;
        if ( $ENV{EVG_ORCH_TEST} ) {
            BAIL_OUT($err);
        }
        if ( $err =~ /couldn't connect|connection refused/i ) {
            $err = "no mongod on " . ( $ENV{MONGOD} || "localhost:27017" );
            $err .= ' and $ENV{MONGOD} not set' unless $ENV{MONGOD};
        }
        plan skip_all => "$err";
    }
}

sub skip_if_mongod {
    eval {
        my $conn = build_client( server_selection_timeout_ms => 1000 );
        my $topo = $conn->_topology;
        $topo->scan_all_servers;
        # will throw if no servers available
        $topo->get_readable_link( MongoDB::ReadPreference->new( { mode => 'nearest' } ) );
    };
    if ( !$@ ) {
        plan skip_all => "Test can't start with a running mongod";
    }
}

sub skip_unless_failpoints_available {
    my ($arg) = @_;

    # Setting failpoints will make the tested server unusable for ordinary
    # purposes. As this is risky, the test requires the user to opt-in
    unless ( $ENV{FAILPOINT_TESTING} ) {
        plan skip_all => "\$ENV{FAILPOINT_TESTING} is false";
    }

    # Test::Harness 3.31 supports the t/testrules.yml file to ensure that
    # this test file won't be run in parallel other tests, since turning on
    # a fail point will interfere with other tests.
    if ( version->parse( $ENV{HARNESS_VERSION} ) < version->parse(3.31) ) {
        plan skip_all => "not safe to run fail points before Test::Harness 3.31";
    }

    # If running from t/ check that the file is in the test rules file.
    if ( $0 =~ m{^t/.*\.t$} ) {
        my $rules = path("t/testrules.yml")->slurp_utf8;
        plan skip_all => "$0 not listed in t/testrules.yml"
          unless $rules =~ m{seq:\s+\Q$0\E};
    }

    my $conn        = build_client;
    my $server_type = server_type($conn);

    my $param = eval {
        $conn->get_database('admin')
          ->run_command( [ getParameter => 1, enableTestCommands => 1 ] );
    };

    plan skip_all => "enableTestCommands is off"
      unless $param && $param->{enableTestCommands};

    # For transaction tests under mongos
    plan skip_all => "fail points not supported via mongos"
      if $server_type eq 'Mongos'
        && ( $conn->_topology->wire_version_ceil < 8
          || $arg->{skip_mongos} );
}

sub set_failpoint {
    my ( $client, $failpoint ) = @_;

    return unless defined $failpoint;
    _send_failpoint_admin_command( $client, [
        configureFailPoint => $failpoint->{configureFailPoint},
        mode => $failpoint->{mode},
        defined $failpoint->{data}
          ? ( data => $failpoint->{data} )
          : (),
    ]);
}

sub clear_failpoint {
    my ( $client, $failpoint ) = @_;

    return unless defined $failpoint;
    _send_failpoint_admin_command( $client, [
        configureFailPoint => $failpoint->{configureFailPoint},
        mode => 'off',
    ]);
}

# Failpoint commands must be sent to all servers if sharded
sub _send_failpoint_admin_command {
    my ( $client, $command ) = @_;

    if ( $client->_topology->type eq 'Sharded' ) {
        for my $server ( $client->_topology->all_servers ) {
            $client->_send_direct_admin_command( $server->address, $command );
        }
    } else {
        $client->send_admin_command( $command );
    }
}

sub skip_unless_sessions {
    my $conn = build_client;

    plan skip_all => "Session support not available"
      unless $conn->_topology->_supports_sessions;
}

sub skip_unless_transactions {
    my $conn = build_client;

    plan skip_all => "Transaction support not available"
      unless $conn->_topology->_supports_transactions;
}

sub server_version {
    my $conn          = shift;
    return $conn->_primary_server_version;
}

sub check_min_server_version {
    my ( $conn, $min_version ) = @_;
    $min_version = "v$min_version" unless $min_version =~ /^v/;
    $min_version .= ".0" unless $min_version =~ /^v\d+\.\d+.\d+$/;
    $min_version = version->new($min_version);
    my $server_version = server_version($conn);
    if ( $min_version > $server_version ) {
        return 1;
    }
    return 0;
}

sub skip_unless_min_version {
    my ( $conn, $min_version ) = @_;
    plan skip_all => "Requires MongoDB $min_version"
        if check_min_server_version( $conn, $min_version );
}

sub server_type {

    my $conn = shift;
    my $server_type;

    # check database type
    my $ismaster = $conn->get_database('admin')->run_command( { ismaster => 1 } );
    if ( exists $ismaster->{msg} && $ismaster->{msg} eq 'isdbgrid' ) {
        $server_type = 'Mongos';
    }
    elsif ( $ismaster->{ismaster} && exists $ismaster->{setName} ) {
        $server_type = 'RSPrimary';
    }
    elsif ( !exists $ismaster->{setName} && !$ismaster->{isreplicaset} ) {
        $server_type = 'Standalone';
    }
    else {
        $server_type = 'Unknown';
    }
    return $server_type;
}

sub get_features {
    my $conn = shift;
    my $topo = $conn->_topology;
    $topo->scan_all_servers;
    my $link;
    eval { $link = $topo->get_writable_link };
    return $link // MongoDB::_Link->new( address => "0:0" );
}

# URI escaping adapted from HTTP::Tiny
my %escapes = map { chr($_) => sprintf( "%%%02X", $_ ) } 0 .. 255;
my $unsafe_char = qr/[^A-Za-z0-9\-\._~]/;

sub uri_escape {
    my ($str) = @_;
    utf8::encode($str);
    $str =~ s/($unsafe_char)/$escapes{$1}/ge;
    return $str;
}

sub to_snake_case {
    my $t = shift;
    $t =~ s{([A-Z])}{_\L$1}g;
    return $t;
}

sub remap_hashref_to_snake_case {
    my $hash = shift;
    return {
        map {
            my $k = to_snake_case($_);
            $k => $hash->{$_}
        } keys %$hash
    };
}

sub uuid_to_string {
    my $uuid = shift;
    return join "-", unpack( "H8H4H4H4H12", $uuid );
}

sub clear_testdbs { @testdbs = () }

# cleanup test dbs
END {
    for my $db (@testdbs) {
        $db->drop;
    }
}

1;
