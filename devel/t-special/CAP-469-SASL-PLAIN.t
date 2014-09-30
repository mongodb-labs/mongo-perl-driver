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

# REQUIRES DRIVER WITH SASL SUPPORT
#
# See also https://wiki.mongodb.com/display/DH/Testing+Kerberos
#
# Set host, username, password in ENV vars: MONGOD, MONGOUSER, MONGOPASS
#
# DO NOT put username:password in MONGOD, as some tests need to see that it
# fails without username/password.

BAIL_OUT("You must set MONGOD, MONGOUSER and MONGOPASS")
    unless 3 == grep { defined $ENV{$_} } qw/MONGOD MONGOUSER MONGOPASS/;

subtest "no authentication" => sub {
    my $conn   = MongoDB::MongoClient->new(
        host => $ENV{MONGOD},
        dt_type => undef,
        server_selection_timeout_ms => 1000,
    );
    my $testdb = $conn->get_database("ldap");
    my $coll   = $testdb->get_collection("test");

    like(
        exception { $coll->count },
        qr/not authorized/,
        "can't read collection when not authenticated"
    );
};

subtest "with authentication" => sub {
    my $conn   = MongoDB::MongoClient->new(
        host => $ENV{MONGOD},
        username => $ENV{MONGOUSER},
        password => $ENV{MONGOPASS},
        auth_mechanism => 'PLAIN',
        dt_type => undef,
        server_selection_timeout_ms => 1000,
    );
    my $testdb = $conn->get_database("ldap");
    my $coll   = $testdb->get_collection("test");

    is( exception { $coll->count }, undef, "no exception reading from new client" );
};

subtest "with legacy sasl attributes" => sub {
    my $conn   = MongoDB::MongoClient->new(
        host => $ENV{MONGOD},
        username => $ENV{MONGOUSER},
        password => $ENV{MONGOPASS},
        sasl     => 1,
        sasl_mechanism => 'PLAIN',
        dt_type => undef,
        server_selection_timeout_ms => 1000,
    );
    my $testdb = $conn->get_database("ldap");
    my $coll   = $testdb->get_collection("test");

    is( exception { $coll->count }, undef, "no exception reading from new client" );
};

my $connect_string = $ENV{MONGOD};
$connect_string =~ s{mongodb://}{mongodb://$ENV{MONGOUSER}:$ENV{MONGOPASS}\@};
$connect_string =~ s{/?$}{};

my @strings = (
    "$connect_string/\$external?authMechanism=PLAIN",
    "$connect_string/?authMechanism=PLAIN&authSource=\$external",
    "$connect_string/?authMechanism=PLAIN",
);

for my $uri ( @strings ) {
    subtest "connect string: $uri" => sub {
        $connect_string .= '$external?authMechanism=PLAIN';
        ok( my $conn   = MongoDB::MongoClient->new(
                host => $uri,
                dt_type => undef,
                server_selection_timeout_ms => 1000,
            ),
            "new client",
        );

        my $testdb = $conn->get_database("ldap");
        my $coll   = $testdb->get_collection("test");

        is( exception { $coll->count }, undef, "no exception reading" );
    };
}

done_testing;
