#
#  Copyright 2009-2013 MongoDB, Inc.
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



package MongoDBTest;

use strict;
use warnings;

use Exporter 'import';
use MongoDB;
use Test::More;
use boolean;
use version;

our @EXPORT_OK = qw(
  build_client get_test_db server_version server_type clear_testdbs get_capped
  skip_unless_mongod uri_escape get_unique_collection
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

    # long query timeout may help spurious failures on heavily loaded CI machines
    return MongoDB->connect(
        $host,
        {
            ssl                         => $ssl || $ENV{MONGO_SSL},
            socket_timeout_ms           => 60000,
            server_selection_timeout_ms => $ENV{ATLAS_PROXY} ? 10000 : 2000,
            server_selection_try_once   => 0,
            %args,
        }
    );
}

sub get_test_db {
    my $conn = shift;
    my $prefix = shift || 'testdb';
    my $testdb = $prefix . int(rand(2**31));
    my $db = $conn->get_database($testdb) or die "Can't get database\n";
    push(@testdbs, $db);
    return  $db;
}

sub get_unique_collection {
    my ( $db, $prefix ) = @_;
    return $db->get_collection(
        sprintf( '%s_%d_%d', $prefix, time(), int(rand(999999)) )
    );
}

sub get_capped {
    my ($db, $name, %args) = @_;
    $name ||= 'capped' . int(rand(2**31));
    $args{size} ||= 500_000;
    $db->run_command([ create => $name, capped => true, %args ]);
    return $db->get_collection($name);
}

sub skip_unless_mongod {
    eval {
        my $conn = build_client( server_selection_timeout_ms => 1000 );
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

sub server_version {

    my $conn = shift;
    my $build = $conn->send_admin_command( [ buildInfo => 1 ] )->output;
    my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
    return version->parse("v$version_str");
}

sub server_type {

    my $conn = shift;
    my $server_type;

    # check database type
    my $ismaster = $conn->get_database('admin')->run_command({ismaster => 1});
    if (exists $ismaster->{msg} && $ismaster->{msg} eq 'isdbgrid') {
        $server_type = 'Mongos';
    }
    elsif ( $ismaster->{ismaster} && exists $ismaster->{setName} ) {
        $server_type = 'RSPrimary'
    }
    elsif ( ! exists $ismaster->{setName} && ! $ismaster->{isreplicaset} ) {
        $server_type = 'Standalone'
    }
    else {
        $server_type = 'Unknown';
    }
    return $server_type;
}

# URI escaping adapted from HTTP::Tiny
my %escapes = map { chr($_) => sprintf("%%%02X", $_) } 0..255;
my $unsafe_char = qr/[^A-Za-z0-9\-\._~]/;

sub uri_escape {
    my ($str) = @_;
    utf8::encode($str);
    $str =~ s/($unsafe_char)/$escapes{$1}/ge;
    return $str;
}

sub clear_testdbs { @testdbs = () }

# cleanup test dbs
END {
    for my $db (@testdbs) {
        $db->drop;
    }
}

1;
