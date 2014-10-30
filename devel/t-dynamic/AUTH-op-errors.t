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
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use Log::Any::Adapter qw/Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs/;
use MongoDB::_URI;

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/mongod-2.6-auth.yml" );
diag "starting server with auth enabled";
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

my $uri = MongoDB::_URI->new( uri => $ENV{MONGOD} );
my $no_auth_string = "mongodb://" . $uri->hostpairs->[0];

# This file is for testing operations that fail when unauthorized.  It uses
# two client connections, one with root permissions ($alice) and one with
# no permissions ($eve)

my $alice = build_client( dt_type => undef ); # root
my $alice_db = get_test_db($alice);
my $alice_coll = $alice_db->get_collection("foo");

my $eve = build_client( host => $no_auth_string, dt_type => undef ); # unauthorized
my $eve_db = get_test_db($eve);
my $eve_coll = $eve_db->get_collection("foo");

subtest "safe and unsafe remove" => sub {
    $alice_coll->drop;
    $alice_coll->insert( {} ) for 1 .. 10;

    my $err = exception { $eve_coll->remove( {}, {safe => 0} ) };
    is( $err, undef, "failed remove with safe => 0 does not throw an error" );

    for my $h ( undef, { safe => 1 } ) {
        $err = exception { $eve_coll->remove( {}, $h ) };
        my $case = $h ? "explicit" : "default";
        like($err->message, qr/not authorized/, "failed remove with $case safe throws exception" );
    }
};

subtest "safe and unsafe save" => sub {
    $alice_coll->drop;

    my $err = exception { $eve_coll->save( { _id => 'foo' }, {safe => 0} ) };
    is( $err, undef, "failed save with safe => 0 does not throw an error" );

    for my $h ( undef, { safe => 1 } ) {
        $err = exception { $eve_coll->save( { _id => 'foo' }, $h ) };
        my $case = $h ? "explicit" : "default";
        like($err->message, qr/not authorized/, "failed save with $case safe throws exception" );
    }
};


clear_testdbs;

done_testing;
