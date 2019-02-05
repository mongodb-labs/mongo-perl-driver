#  Copyright 2017 - present MongoDB, Inc.
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
use JSON::MaybeXS;
use YAML::XS qw/LoadFile DumpFile/;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;
use Try::Tiny;
use Sys::Hostname;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;

use MongoDBTest qw/build_client get_test_db clear_testdbs server_version
  server_type skip_if_mongod skip_unless_mongod/;

# This test starts servers on localhost ports 27017, 27018 and 27019. We skip if
# these aren't available.
for my $port ( 27017, 27018, 27019 ) {
    local $ENV{MONGOD} = "mongodb://localhost:$port/";
    skip_if_mongod();
}

my $cert_dir = $ENV{MONGO_TEST_CERT_PATH} || 'devel/certs';
my $cert_user = $ENV{MONGO_TEST_CERT_USER}
  || "CN=client,OU=Drivers,O=MongoDB,L=New York,ST=New York,C=US";
my $server_cn = "CN=localhost,OU=Server,O=MongoDB,L=New York,ST=New York,C=US";

my %certs = (
    client    => "$cert_dir/client.pem",
    ca        => "$cert_dir/ca.pem",
    server    => "$cert_dir/server.pem",
);

my $config  = LoadFile("devel/config/replicaset-any-27017.yml");
$config->{ssl_config} = {
    # not require, as need to connect without SSL for one of the tests
    mode     => 'preferSSL',
    servercn => $server_cn,
    certs    => { map { $_ => $certs{$_} } qw/server ca client/ },
};
$config->{auth} = {
    user => "auser",
    password => "apass",
};
my $config_path = Path::Tiny->tempfile;
DumpFile( "$config_path", $config );

my %PO_keymap = (
    user => 'username',
    password => 'password',
    auth_database => 'db_name',
);

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "$config_path" );
$orc->start;

# PERL-1061 explicit ssl with mongodb+serv
subtest "mongodb+srv with boolean options" => sub {
  for my $key ( qw/ssl serverSelectionTryOnce/ ) {
    eval { MongoDB->connect("mongodb+srv://test1.test.build.10gen.cc/?replicaSet=repl0&$key=true") };
    is( $@, "", "explicit '$key=true' in URI should not error" );
    eval { MongoDB->connect("mongodb+srv://test1.test.build.10gen.cc/?replicaSet=repl0&$key=false") };
    is( $@, "", "explicit '$key=false' in URI should not error" );
  }
};

sub new_client {
    my $host = shift;
    return build_client(
        host    => $host,
        dt_type => undef,
        ssl            => {
            SSL_ca_file         => $certs{ca},
            SSL_verifycn_scheme => 'none',
        },
    );
}

sub run_test {
    my $test = shift;

    if ( $test->{error} ) {
        # This test should error the parsing step at some point
        isnt( exception { new_client( $test->{uri} ) }, undef,
            "invalid uri" );
        return;
    }

    my $mongo;
    eval {
      $mongo = new_client( $test->{uri} );
    };
    my $err = $@;
    isa_ok( $mongo, 'MongoDB::MongoClient' ) or diag "Error: $err";
    # drop out of test to save on undef errors - its already failed
    return unless defined $mongo;
    my $uri = $mongo->_uri;

    if ( defined $test->{options} ) {
        my $lc_opts = { map { lc $_ => $test->{options}->{$_} } keys %{ $test->{options} } };
        # force ssl JSON boolean to perlish
        $lc_opts->{ssl} = $lc_opts->{ssl} ? 1 : 0;
        is_deeply( $uri->options, $lc_opts, "options are correct" );
    }
    is_deeply( [ sort @{ $uri->hostids } ], [ sort @{ $test->{seeds} } ], "seeds are correct" );
    my $topology = $mongo->topology_status( refresh => 1 );
    my @found_servers = map { $_->{address} } @{ $topology->{servers} };
    my $hostname = hostname();
    my @wanted_servers = map { ( my $h = $_ ) =~ s/localhost/$hostname/; $h } @{ $test->{hosts} };
    is_deeply( [ sort @found_servers ], [ sort @wanted_servers ], "hosts are correct" );

    for my $k ( keys %{ $test->{parsed_options} || {}} ) {
        my $meth = $PO_keymap{$k};
        if ( defined $meth ) {
            is( $uri->$meth, $test->{parsed_options}{$k}, "parsed '$k' is correct" );
        }
        else {
            fail("Unknown parsed option '$k'");
        }
    }
}

my $dir      = path("t/data/initial_dns_seedlist_discovery");
my $iterator = $dir->iterator;
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    subtest $path => sub {
        my $description = $plan->{comment};
        subtest $description => sub {
            run_test( $plan );
        }
    };
}

clear_testdbs;

done_testing;
