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
use MongoDBTest qw/build_client get_test_db clear_testdbs uri_escape/;
use MongoDB::_URI;

#--------------------------------------------------------------------------#
# constants
#--------------------------------------------------------------------------#

# Get names from certs like this:
# openssl x509 -inform PEM -subject -nameopt RFC2253 -in devel/certs/client.pem

my $cert_dir = $ENV{MONGO_TEST_CERT_PATH} || 'devel/certs';
my $cert_user = $ENV{MONGO_TEST_CERT_USER}
  || "CN=client,OU=Drivers,O=MongoDB,L=New York,ST=New York,C=US";
my $bad_user  = "CN=client,OU=Legal,O=Evil Corp,L=Los Angeles,ST=California,C=US";
my $server_cn = "CN=localhost,OU=Server,O=MongoDB,L=New York,ST=New York,C=US";
my $safe_cert_user = uri_escape($cert_user);

my %certs = (
    client    => "$cert_dir/client.pem",
    badclient => "$cert_dir/badclient.pem",
    ca        => "$cert_dir/ca.pem",
    server    => "$cert_dir/server.pem",
);

my $valid_msg   = "can read from collection when authenticated";
my $invalid_msg = "can't read from collection when not authenticated";

#--------------------------------------------------------------------------#
# support functions
#--------------------------------------------------------------------------#

# set up config file for a server version customized with certificate paths

sub customize_config {
    my $version = shift;
    my $config  = LoadFile("devel/config/mongod-$version.yml");
    $config->{ssl_config} = {
        mode     => 'requireSSL',
        username => $cert_user,
        servercn => $server_cn,
        certs    => { map { $_ => $certs{$_} } qw/server ca client/ },
    };
    my $config_path = Path::Tiny->tempfile;
    DumpFile( "$config_path", $config );
    return $config_path;
}

# bring up server with a specified config file

sub launch_server {
    my $config_path = shift;
    my $orc = MongoDBTest::Orchestrator->new( config_file => "$config_path" );
    $orc->start;
    return $orc;
}

# create client with a given client cert and optional username

sub new_client {
    my ( $host, $cert_file, $username ) = @_;
    return build_client(
        host    => $host,
        dt_type => undef,
        ( $username ? ( username => $username ) : () ),
        auth_mechanism => 'MONGODB-X509',
        ssl            => {
            SSL_cert_file       => $cert_file,
            SSL_ca_file         => $certs{ca},
            SSL_verifycn_scheme => 'none',
        },
    );
}

# extract last authentication from log

sub last_auth_line {
    my ($orc)       = @_;
    my ($a_server)  = $orc->deployment->all_servers;
    my @log_lines   = $a_server->logfile->lines;
    my ($last_auth) = reverse grep /D COMMAND.*MONGODB-X509/, @log_lines;
    return $last_auth;
}

#--------------------------------------------------------------------------#
# Test X509 authentication with username provided
#--------------------------------------------------------------------------#

# When a username is provided, server versions before/after 3.4 should have
# the same behavior.  With usernames, we must also consider the possibility
# of names/certificates matching or being mismatched.

for my $server_version (qw/3.2 3.4/) {

    subtest "Server $server_version with username provided" => sub {
        my $orc = launch_server( customize_config($server_version) );
        my $uri = $orc->as_uri;

        subtest "invalid client cert (matching username)" => sub {
            my $conn = new_client( $uri, $certs{badclient}, $bad_user );
            my $coll = $conn->ns("x509.test_collection");

            like( exception { $coll->count }, qr/MongoDB::AuthError/, $invalid_msg );
        };

        subtest "invalid client cert, but valid username" => sub {
            my $conn = new_client( $uri, $certs{badclient}, $cert_user );
            my $coll = $conn->ns("x509.test_collection");

            like( exception { $coll->count }, qr/MongoDB::AuthError/, $invalid_msg );
        };

        subtest "valid client cert, but bad username" => sub {
            my $conn = new_client( $uri, $certs{client}, $bad_user );
            my $coll = $conn->ns("x509.test_collection");

            like( exception { $coll->count }, qr/MongoDB::AuthError/, $invalid_msg );
        };

        subtest "valid client cert (matching username in attributes)" => sub {
            my $conn = new_client( $uri, $certs{client}, $cert_user );
            my $coll = $conn->ns("x509.test_collection");

            is( exception { $coll->count }, undef, $valid_msg );
        };

        subtest "valid client cert (matching username in URL)" => sub {
            ( my $new_uri = $uri ) =~ s{://}{://$safe_cert_user@};

            my $conn = new_client( $new_uri, $certs{client} );
            my $coll = $conn->ns("x509.test_collection");

            is( exception { $coll->count }, undef, $valid_msg );
        };

    };
}

#--------------------------------------------------------------------------#
# Test X509 authentication without username provided
#--------------------------------------------------------------------------#

# Without username, we expect different behavior on servers before/after 3.4.
# For earlier servers, the driver find the username from the client cert and
# sends it.  For 3.4+, the server will do that, so that driver sends the
# authentication command without a 'user' argument.

subtest "Server 3.2 with no username provided" => sub {
    my $orc = launch_server( customize_config("3.2") );
    my $uri = $orc->as_uri;

    subtest "valid client cert" => sub {
        my $conn = new_client( $uri, $certs{client} );
        my $coll = $conn->ns("x509.test_collection");

        is( exception { $coll->count }, undef, $valid_msg );
        like(
            last_auth_line($orc),
            qr/user: "\Q$cert_user\E"/,
            "authenticate command had username"
        );
    };

    subtest "invalid client cert" => sub {
        my $conn = new_client( $uri, $certs{badclient} );
        my $coll = $conn->ns("x509.test_collection");

        like( exception { $coll->count }, qr/MongoDB::AuthError/, $invalid_msg );
    };
};

subtest "Server 3.4 with no username provided" => sub {
    my $orc = launch_server( customize_config("3.4") );
    my $uri = $orc->as_uri;

    subtest "valid client cert" => sub {
        my $conn = new_client( $uri, $certs{client} );
        my $coll = $conn->ns("x509.test_collection");

        is( exception { $coll->count }, undef, $valid_msg );
        unlike( last_auth_line($orc), qr/user:/, "authenticate command had no username" );
    };

    subtest "invalid client cert" => sub {
        my $conn = new_client( $uri, $certs{badclient} );
        my $coll = $conn->ns("x509.test_collection");

        like( exception { $coll->count }, qr/MongoDB::AuthError/, $invalid_msg );
    };

};

#--------------------------------------------------------------------------#
# cleanup
#--------------------------------------------------------------------------#

clear_testdbs;

done_testing;
