#
#  Copyright 2014 MongoDB, Inc.
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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use YAML::XS qw/LoadFile DumpFile/;
use Path::Tiny;
use boolean;

use IO::Socket::SSL; # initialize early to allow for debugging mode
BEGIN { $IO::Socket::SSL::DEBUG = $ENV{SSLVERBOSE} }

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs/;
use MongoDB::_URI;

my $cert_dir = $ENV{MONGO_TEST_CERT_PATH} || 'devel/certs';
my $cert_user = $ENV{MONGO_TEST_CERT_USER}
  || "CN=TEST-CLIENT,OU=QAClient,O=MongoDB,ST=California,C=US";
my $bad_user = "CN=TEST-CLIENT-2,OU=QAClient,O=MongoDB,ST=California,C=US";

my %certs = (
    client    => "$cert_dir/client.pem",
    badclient => "$cert_dir/client2.pem",
    ca        => "$cert_dir/ca.pem",
    server    => "$cert_dir/server.pem",
    crl       => "$cert_dir/crl-server.pem",
);

#--------------------------------------------------------------------------#
# set up config file customized with certificate paths
#--------------------------------------------------------------------------#

my $config = LoadFile("devel/config/mongod-3.0.yml");
$config->{ssl_config} = {
    mode     => 'requireSSL',
    username => $cert_user,
    servercn => 'server',
    certs    => { map { $_ => $certs{$_} } qw/server ca client/ },
};
my $config_path = Path::Tiny->tempfile;
DumpFile( "$config_path", $config );

#--------------------------------------------------------------------------#
# bring up server with config
#--------------------------------------------------------------------------#

my $orc = MongoDBTest::Orchestrator->new( config_file => "$config_path" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

#--------------------------------------------------------------------------#
# start testing
#--------------------------------------------------------------------------#

my $uri = MongoDB::_URI->new( uri => $ENV{MONGOD} );
my $no_auth_string = "mongodb://" . $uri->hostids->[0];

subtest "invalid client" => sub {
    my $conn = build_client(
        host           => $uri->uri,
        dt_type        => undef,
        username       => $bad_user,
        auth_mechanism => 'MONGODB-X509',
        ssl            => {
            SSL_cert_file       => $certs{badclient},
            SSL_ca_file         => $certs{ca},
            SSL_verifycn_scheme => 'none',
        },
    );
    my $testdb = $conn->get_database("x509");
    my $coll   = $testdb->get_collection("test_collection");

    like(
        exception { $coll->count },
        qr/MongoDB::AuthError/,
        "can't read collection when not authenticated"
    );
};

subtest "auth via client attributes" => sub {
    my $conn = build_client(
        host           => $no_auth_string,
        username       => $cert_user,
        dt_type        => undef,
        auth_mechanism => 'MONGODB-X509',
        ssl            => {
            SSL_cert_file       => $certs{client},
            SSL_ca_file         => $certs{ca},
            SSL_verifycn_scheme => 'none',
        }
    );
    my $testdb = $conn->get_database("x509");
    my $coll   = $testdb->get_collection("test_collection");

    is( exception { $coll->count }, undef, "no exception reading from new client" );
};

clear_testdbs;

done_testing;
