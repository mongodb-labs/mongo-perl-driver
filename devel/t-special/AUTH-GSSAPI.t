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
use boolean;

use MongoDB;

# REQUIRES DRIVER WITH SASL SUPPORT: Authen::SASL and either
# the GSSAPI module or the Authen::SASL::XS module
#
# Test setup designed for the MongoDB QA repo vagrant boxes
#
# Or, vagrant can set up host names in /etc/hosts and copy
# /etc/krb5.config from one of the vagrant boxes.
#
# Be sure to run kinit for the username below

subtest "no auth" => sub {
    my $mc = MongoDB::MongoClient->new(
        host                        => 'mongodb://rhel64.mongotest.com/',
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("test")->get_collection("foo");

    like(
        exception { ok( $coll->insert( { name => 'johndoe' } ), "insert" ) },
        qr/not authorized/,
        "insert failed due to auth error",
    );
};

subtest "auth fails" => sub {
    my $mc = MongoDB::MongoClient->new(
        host                        => 'mongodb://rhel64.mongotest.com/',
        username                    => 'bogus@MONGOTEST.COM',
        auth_mechanism              => 'GSSAPI',
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("test")->get_collection("foo");

    # XXX eventually, this should fail fast with a better message
    like(
        exception { ok( $coll->insert( { name => 'johndoe' } ), "insert" ) },
        qr/No writable server/,
        "insert failed because no server authenticated",
    );

};

subtest "auth OK via attributes" => sub {
    my $mc = MongoDB::MongoClient->new(
        host                        => 'mongodb://rhel64.mongotest.com/',
        username                    => 'gssapitest@MONGOTEST.COM',
        auth_mechanism              => 'GSSAPI',
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("test")->get_collection("foo");
    ok( $coll->insert( { name => 'johndoe' } ), "insert" );
    ok( $coll->find_one( { name => 'johndoe' } ), "find" );
};

subtest "auth OK via connect string" => sub {
    my $mc = MongoDB::MongoClient->new(
        host =>
          'mongodb://gssapitest%40MONGOTEST.COM@rhel64.mongotest.com/?authMechanism=GSSAPI',
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("test")->get_collection("foo");
    ok( $coll->insert( { name => 'johndoe' } ), "insert" );
    ok( $coll->find_one( { name => 'johndoe' } ), "find" );
};

subtest "auth fails via connect string to wrong realm" => sub {
    my $mc = MongoDB::MongoClient->new(
        host =>
          'mongodb://gssapitest%40MONGOTEST.COM@rhel64.mongotest.com/?authMechanism=GSSAPI&authMechanism.SERVICE_NAME=mongo',
        server_selection_timeout_ms => 1000,
    );

    my $coll = $mc->get_database("test")->get_collection("foo");

    # XXX eventually, this should fail fast with a better message
    like(
        exception { ok( $coll->insert( { name => 'johndoe' } ), "insert" ) },
        qr/No writable server/,
        "insert failed because no server authenticated",
    );

};

done_testing;
