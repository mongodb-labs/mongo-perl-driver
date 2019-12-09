#  Copyright 2015 - present MongoDB, Inc.
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
        client_cert_not_required => 1,
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
# URI vs OO attribute TLS configuration
#--------------------------------------------------------------------------#
subtest "URI vs OO attribute config" => sub {
    my $orc = launch_server( customize_config("3.4") );
    my $uri = $orc->as_uri;

    # naming: "URI; OO" options
    my $cases = [
        # URI tls undef
        {
            name       => "undef; undef",
            uri_tls    => "",
            opt_tls    => undef,
            expect_ssl => 0,
            error_like => qr/MongoDB::NetworkError/,
        },
        {
            name       => "undef; 0",
            uri_tls    => "",
            opt_tls    => 0,
            expect_ssl => 0,
            error_like => qr/MongoDB::NetworkError/,
        },
        {
            name       => "undef; 1",
            uri_tls    => "",
            opt_tls    => 1,
            expect_ssl => 1,
            error_like => qr/MongoDB::HandshakeError/,
        },
        {
            name       => "undef; SSL_verify_mode=0",
            uri_tls    => "",
            opt_tls    => { SSL_verify_mode => 0x00 },
            expect_ssl => { SSL_verify_mode => 0x00 },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },

        # URI tls false
        {
            name       => "tls=false; SSL_verify_mode=0",
            uri_tls    => "tls=false",
            opt_tls    => { SSL_verify_mode => 0x00 },
            expect_ssl => 0,
            error_like => qr/MongoDB::NetworkError/,
        },
        {
            name       => "ssl=false; SSL_verify_mode=0",
            uri_tls    => "ssl=false",
            opt_tls    => { SSL_verify_mode => 0x00 },
            expect_ssl => 0,
            error_like => qr/MongoDB::NetworkError/,
        },

        # URI tls true
        {
            name       => "tls=true; 0",
            uri_tls    => "tls=true",
            opt_tls    => 0,
            expect_ssl => 1,
            error_like => qr/MongoDB::HandshakeError/,
        },
        {
            name       => "ssl=true; 0",
            uri_tls    => "ssl=true",
            opt_tls    => 0,
            expect_ssl => 1,
            error_like => qr/MongoDB::HandshakeError/,
        },
        {
            name       => "tls=true; SSL_verify_mode=0",
            uri_tls    => "tls=true",
            opt_tls    => { SSL_verify_mode => 0x00 },
            expect_ssl => { SSL_verify_mode => 0x00 },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },
        {
            name       => "ssl=true; SSL_verify_mode=0",
            uri_tls    => "ssl=true",
            opt_tls    => { SSL_verify_mode => 0x00 },
            expect_ssl => { SSL_verify_mode => 0x00 },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },

        # URI tls with options (but not client certs)
        {
            name       => "tls=true&tlsInsecure=true; 0",
            uri_tls    => "tls=true&tlsInsecure=true",
            opt_tls    => 0,
            expect_ssl => { SSL_verify_mode => 0x00, SSL_verifycn_scheme => "none" },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },
        {
            name       => "tlsInsecure=true; 0",
            uri_tls    => "tlsInsecure=true",
            opt_tls    => 0,
            expect_ssl => { SSL_verify_mode => 0x00, SSL_verifycn_scheme => "none" },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },
        {
            name       => "tlsInsecure=true; SSL_verify_mode=0",
            uri_tls    => "tlsInsecure=true",
            opt_tls    => { SSL_verify_mode => 0x01 },
            expect_ssl => { SSL_verify_mode => 0x00, SSL_verifycn_scheme => "none" },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },
        {
            name       => "tlsAllowInvalidHostNames=true&tlsCAFile=<path>; undef",
            uri_tls    => "tlsAllowInvalidHostNames=true&tlsCAFile=$certs{ca}",
            opt_tls    => undef,
            expect_ssl => {
                SSL_ca_file => $certs{ca},
                SSL_verifycn_scheme => "none",
            },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },
        {
            name       => "tlsAllowInvalidCertificates; undef",
            uri_tls    => "tlsAllowInvalidCertificates=true",
            opt_tls    => undef,
            expect_ssl => { SSL_verify_mode => 0x00 },
            error_like => qr/MongoDB::DatabaseError: not authorized/,
        },
        {
            name       => "tlsCertificateKeyFilePassword; undef",
            uri_tls    => "tlsCertificateKeyFilePassword=password",
            opt_tls    => undef,
            expect_ssl => {
              SSL_passwd_cb => "password",
            },
            error_like => qr/MongoDB::HandshakeError/,
        },

        # URI tls with options (with certs and X509 auth)
        {
            name       => "tlsInsecure=true&tlsCertificateKeyFile=<path>; undef",
            uri_tls    => "tlsInsecure=true&tlsCertificateKeyFile=$certs{client}&authMechanism=MONGODB-X509",
            opt_tls    => undef,
            expect_ssl => {
              SSL_verify_mode => 0x00,
              SSL_verifycn_scheme => "none",
              SSL_cert_file => $certs{client},
            },
            error_like => undef,
        },

    ];

    for my $c (@$cases) {
        subtest $c->{name}, sub {
            local $SIG{__WARN__} = sub { 0 };
            local *MongoDB::_Constants::WITH_ASSERTS = "";
            my $test_uri = $uri . "/?$c->{uri_tls}";
            my $mc       = MongoDB->connect( $test_uri,
                defined $c->{opt_tls} ? { ssl => $c->{opt_tls} } : () );

            if (ref $c->{expect_ssl} && exists $c->{expect_ssl}{SSL_passwd_cb}) {
              my $pwd = delete $c->{expect_ssl}{SSL_passwd_cb};
              my $cb = delete $mc->{ssl}{SSL_passwd_cb};
              is( ref $cb, "CODE", "password callback is code")
                && is( $cb->(), $pwd, "callback gave correct password");
            }
            is_deeply( $mc->{ssl}, $c->{expect_ssl}, "ssl attribute" );

            my $coll = $mc->ns("x509.test_collection");
            my $err = exception { $coll->insert_one({}) };
            if ( defined $c->{error_like} ) {
                like( $err, $c->{error_like}, "insert should error with $c->{error_like}" );
            }
            else {
                is( $err, undef, "insert_one should not error" );
            }
        };
    }
};

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

            like( exception { $coll->count_documents({}) }, qr/MongoDB::AuthError/, $invalid_msg );
        };

        subtest "invalid client cert, but valid username" => sub {
            my $conn = new_client( $uri, $certs{badclient}, $cert_user );
            my $coll = $conn->ns("x509.test_collection");

            like( exception { $coll->count_documents({}) }, qr/MongoDB::AuthError/, $invalid_msg );
        };

        subtest "valid client cert, but bad username" => sub {
            my $conn = new_client( $uri, $certs{client}, $bad_user );
            my $coll = $conn->ns("x509.test_collection");

            like( exception { $coll->count_documents({}) }, qr/MongoDB::AuthError/, $invalid_msg );
        };

        subtest "valid client cert (matching username in attributes)" => sub {
            my $conn = new_client( $uri, $certs{client}, $cert_user );
            my $coll = $conn->ns("x509.test_collection");

            is( exception { $coll->count_documents({}) }, undef, $valid_msg );
        };

        subtest "valid client cert (matching username in URL)" => sub {
            ( my $new_uri = $uri ) =~ s{://}{://$safe_cert_user@};

            my $conn = new_client( $new_uri, $certs{client} );
            my $coll = $conn->ns("x509.test_collection");

            is( exception { $coll->count_documents({}) }, undef, $valid_msg );
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

        is( exception { $coll->count_documents({}) }, undef, $valid_msg );
        like(
            last_auth_line($orc),
            qr/user: "\Q$cert_user\E"/,
            "authenticate command had username"
        );
    };

    subtest "invalid client cert" => sub {
        my $conn = new_client( $uri, $certs{badclient} );
        my $coll = $conn->ns("x509.test_collection");

        like( exception { $coll->count_documents({}) }, qr/MongoDB::AuthError/, $invalid_msg );
    };
};

subtest "Server 3.4 with no username provided" => sub {
    my $orc = launch_server( customize_config("3.4") );
    my $uri = $orc->as_uri;

    subtest "valid client cert" => sub {
        my $conn = new_client( $uri, $certs{client} );
        my $coll = $conn->ns("x509.test_collection");

        is( exception { $coll->count_documents({}) }, undef, $valid_msg );
        unlike( last_auth_line($orc), qr/user:/, "authenticate command had no username" );
    };

    subtest "invalid client cert" => sub {
        my $conn = new_client( $uri, $certs{badclient} );
        my $coll = $conn->ns("x509.test_collection");

        like( exception { $coll->count_documents({}) }, qr/MongoDB::AuthError/, $invalid_msg );
    };

};

#--------------------------------------------------------------------------#
# cleanup
#--------------------------------------------------------------------------#

clear_testdbs;

done_testing;
