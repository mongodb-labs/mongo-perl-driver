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

use MongoDB;
use IO::Socket::SSL; # initialize early to allow for debugging mode

# REQUIRES IO::Socket::SSL and SSL-enabled mongod set in MONGOD
#
# This test is designed for use with vagrant ssl box 'mongod'
# from the QA repository and corresponding certificates.  The
# 'server.pem' certificate must be used, as it uses the CN "TEST-SERVER"
#
# Configure the mongod.conf with the following:
# sslPEMKeyFile = /vagrant/certs/server.pem
# sslCAFile = /vagrant/certs/ca.pem
# sslCRLFile = /vagrant/certs/crl-client.pem

$ENV{MONGOD}         ||= 'mongodb://localhost:27017';
$ENV{GOOD_CERT_PATH} ||= '../QA/vagrant/ssl/certs/client.pem';
$ENV{BAD_CERT_PATH}  ||= '../QA/vagrant/ssl/certs/client2.pem';
$ENV{GOOD_CA_PATH}   ||= '../QA/vagrant/ssl/certs/ca.pem';
$ENV{BAD_CA_PATH}    ||= '../QA/vagrant/ssl/certs/ca_invalid.pem';

$IO::Socket::SSL::DEBUG = 0;

subtest "valid cert and CA" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{GOOD_CA_PATH},
                SSL_verifycn_name => 'TEST-SERVER',
                SSL_cert_file     => $ENV{GOOD_CERT_PATH},
            }
        ],
    );

    is( exception { $mc->send_admin_command( [ ismaster => 1 ] ) },
        undef, "ismaster succeeded" );
};

subtest "bad CA" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{BAD_CA_PATH},
                SSL_verifycn_name => 'TEST-SERVER',
                SSL_cert_file     => $ENV{GOOD_CERT_PATH},
            }
        ],
    );

    like(
        exception { $mc->send_admin_command( [ ismaster => 1 ] ) },
        qr/No readable server/,
        "server selection failed"
    );
};

subtest "CN doesn't match" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{BAD_CA_PATH},
                SSL_cert_file     => $ENV{GOOD_CERT_PATH},
            }
        ],
    );

    like(
        exception { $mc->send_admin_command( [ ismaster => 1 ] ) },
        qr/No readable server/,
        "server selection failed"
    );
};

subtest "no CA" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_verifycn_name => 'TEST-SERVER',
                SSL_cert_file     => $ENV{GOOD_CERT_PATH},
            }
        ],
    );

    like(
        exception { $mc->send_admin_command( [ ismaster => 1 ] ) },
        qr/No readable server/,
        "server selection failed"
    );
};

subtest "no cert" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{GOOD_CA_PATH},
                SSL_verifycn_name => 'TEST-SERVER',
            }
        ],
    );

    like(
        exception { $mc->send_admin_command( [ ismaster => 1 ] ) },
        qr/No readable server/,
        "server selection failed"
    );
};

subtest "revoked client cert" => sub {

    my $mc = new_ok(
        "MongoDB::MongoClient",
        [
            host                        => $ENV{MONGOD},
            server_selection_timeout_ms => 1000,
            ssl                         => {
                SSL_ca_file       => $ENV{GOOD_CA_PATH},
                SSL_verifycn_name => 'TEST-SERVER',
                SSL_cert_file     => $ENV{BAD_CERT_PATH},
            }
        ],
    );

    like(
        exception { $mc->send_admin_command( [ ismaster => 1 ] ) },
        qr/No readable server/,
        "server selection failed"
    );
};

done_testing;
