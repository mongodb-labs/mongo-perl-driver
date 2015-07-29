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

use strict;
use warnings;
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use boolean;

use lib 'devel/lib';

use if $ENV{VERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDB;
use MongoDBTest::Orchestrator;
use IO::Socket::SSL; # initialize early to allow for debugging mode

# REQUIRES IO::Socket::SSL and SSL-enabled mongod set in MONGOD
#
#
# This test is designed for use with vagrant x509 box 'mongod' from the QA
# repository and corresponding certificates.  The 'server.pem' certificate must
# be used, as it uses the CN "TEST-SERVER"
#
# Configure the mongod.conf without any ssl fields, then run mongod, run mongo
# and add the test user as follows:
#
# db.getSiblingDB("$external").runCommand( { createUser:
# "CN=TEST-CLIENT,OU=QAClient,O=MongoDB,ST=California,C=US", roles: [ { role:
# 'readWrite', db: 'x509' }, ], writeConcern: { w: "majority" , wtimeout: 5000
# } })
#
# Then Configure the mongod.conf with the following:
#
# sslPEMKeyFile = /vagrant/certs/server.pem
# sslCAFile = /vagrant/certs/ca.pem
# sslCRLFile = /vagrant/certs/crl-client.pem

$ENV{MONGOD}         ||= 'mongodb://192.168.19.100/';
$ENV{GOOD_CERT_PATH} ||= '../QA/vagrant/x509/certs/client.pem';
$ENV{BAD_CERT_PATH}  ||= '../QA/vagrant/x509/certs/client2.pem';
$ENV{GOOD_CA_PATH}   ||= '../QA/vagrant/x509/certs/ca.pem';
$ENV{MONGOUSER}      ||= "CN=TEST-CLIENT,OU=QAClient,O=MongoDB,ST=California,C=US";

( my $BAD_USERNAME = $ENV{MONGOUSER} ) =~ s/C=US/C=UK/;

$IO::Socket::SSL::DEBUG = 0;

# Test that MONGODB-X509 can be specified as auth mechanism in the driver's
# authenticate helper and that authentication is successful with a properly
# configured server, valid certificate, and matching username.

subtest "valid X509, in parameters" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{GOOD_CA_PATH},
                SSL_cert_file     => $ENV{GOOD_CERT_PATH},
                SSL_verifycn_name => 'TEST-SERVER',
            },
            auth_mechanism => 'MONGODB-X509',
            username       => $ENV{MONGOUSER},
        ],
    );

    my $coll = $mc->get_database("x509")->get_collection("foo");
    my $doc;
    is( exception { $coll->insert( { x => 1 } ) }, undef, "insert succeeded" );
    is( exception { $doc = $coll->find_one( { x => 1 } ) }, undef, "find succeeded" );
    is( $doc->{x}, 1, "got right document" );
};

# Test that MONGODB-X509 can be specified as authMechanism in the URI and that
# authentication is successful with a properly configured server instance,
# valid certificate, and matching username.

subtest "valid X509, in URI" => sub {

    my $connect_string = $ENV{MONGOD};
    $connect_string =~ s{mongodb://}{mongodb://$ENV{MONGOUSER}\@};
    $connect_string =~ s{/?$}{};
    $connect_string .= "/?authMechanism=MONGODB-X509";

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $connect_string,
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{GOOD_CA_PATH},
                SSL_cert_file     => $ENV{GOOD_CERT_PATH},
                SSL_verifycn_name => 'TEST-SERVER',
            },
        ],
    );

    my $coll = $mc->get_database("x509")->get_collection("foo");
    my $doc;
    is( exception { $coll->insert( { x => 1 } ) }, undef, "insert succeeded" );
    is( exception { $doc = $coll->find_one( { x => 1 } ) }, undef, "find succeeded" );
    is( $doc->{x}, 1, "got right document" );
};

# Test that the driver requires a username when using MONGODB-X509 in the above
# cases.

subtest "missing username" => sub {

    like(
        exception {
            my $mc = MongoDB::MongoClient->new(
                host                        => $ENV{MONGOD},
                server_selection_timeout_ms => 1000,
                ssl                         => {
                    SSL_ca_file       => $ENV{GOOD_CA_PATH},
                    SSL_cert_file     => $ENV{GOOD_CERT_PATH},
                    SSL_verifycn_name => 'TEST-SERVER',
                },
                auth_mechanism => 'MONGODB-X509',
            );
        },
        qr/invalid field username \(''\)/,
        "missing name is fatal"
    );

};

# Test with a valid certificate and a username that doesn't match - expected
# graceful failure raising the servers error message or another helpful
# message.

subtest "invalid X509 name" => sub {

    like(
        exception {
            my $mc = MongoDB::MongoClient->new(
                host                        => $ENV{MONGOD},
                server_selection_timeout_ms => 1000,
                ssl                         => {
                    SSL_ca_file       => $ENV{GOOD_CA_PATH},
                    SSL_cert_file     => $ENV{GOOD_CERT_PATH},
                    SSL_verifycn_name => 'TEST-SERVER',
                },
                auth_mechanism => 'MONGODB-X509',
                username       => $BAD_USERNAME,
            );
        },
        qr/Authentication.*failed/,
        "auth fails with useful error"
    );
};

# Test with invalid certificate and valid username - expected failure as above.

subtest "invalid X509 cert" => sub {

    like(
        exception {
            my $mc = MongoDB::MongoClient->new(
                host                        => $ENV{MONGOD},
                server_selection_timeout_ms => 1000,
                ssl                         => {
                    SSL_ca_file       => $ENV{BAD_CA_PATH},
                    SSL_cert_file     => $ENV{GOOD_CERT_PATH},
                    SSL_verifycn_name => 'TEST-SERVER',
                },
                auth_mechanism => 'MONGODB-X509',
                username       => $ENV{MONGOUSER},
            );
            my $coll = $mc->get_database("x509")->get_collection("foo");
            $coll->insert( { x => 1 } );
        },
        qr/SSL connection failed/,
        "auth fails with useful error"
    );
};

# Test that the driver raises a helpful error message / exception when
# MONGODB-X509 is used with a server configured without SSL support.

subtest "X509 without SSL server" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
    $orc->start;

    like(
        exception {
            my $mc = MongoDB::MongoClient->new(
                host                        => $orc->as_uri,
                server_selection_timeout_ms => 1000,
                ssl                         => {
                    SSL_ca_file       => $ENV{GOOD_CA_PATH},
                    SSL_cert_file     => $ENV{GOOD_CERT_PATH},
                    SSL_verifycn_name => 'TEST-SERVER',
                },
                auth_mechanism => 'MONGODB-X509',
                username       => $ENV{MONGOUSER},
            );
            my $coll = $mc->get_database("x509")->get_collection("foo");
            $coll->insert( { x => 1 } );
        },
        qr/SSL connection failed/,
        "auth fails with useful error"
    );
};

done_testing;
