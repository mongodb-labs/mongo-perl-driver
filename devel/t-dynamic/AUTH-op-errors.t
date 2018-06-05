#  Copyright 2014 - present MongoDB, Inc.
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
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs/;
use MongoDB::_URI;

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/mongod-2.6-auth.yml" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;

my $uri = MongoDB::_URI->new( uri => $ENV{MONGOD} );
my $no_auth_string = "mongodb://" . $uri->hostids->[0];

# This file is for testing operations that fail when unauthorized.  It uses
# two client connections, one with root permissions ($alice) and one with
# no permissions ($eve)

my $alice = build_client( dt_type => undef ); # root
my $alice_db = get_test_db($alice);
my $alice_coll = $alice_db->get_collection("foo");

my $eve = build_client( host => $no_auth_string, dt_type => undef ); # unauthorized
my $eve_db = get_test_db($eve);
my $eve_coll = $eve_db->get_collection("foo");
my $unsafe_eve_coll = $eve_coll->clone( write_concern => { w => 0 } );

subtest "safe and unsafe remove" => sub {
    $alice_coll->drop;
    $alice_coll->insert_one( {} ) for 1 .. 10;

    my $err = exception { $unsafe_eve_coll->delete_one( {} ) };
    is( $err, undef, "failed remove with w => 0 does not throw an error" );

    $err = exception { $eve_coll->delete_one( {} ) };
    like($err->message, qr/not authorized/, "failed remove with default w throws exception" );
};

subtest "safe and unsafe insert" => sub {
    $alice_coll->drop;

    my $err = exception { $unsafe_eve_coll->insert_one( { _id => 'foo' } ) };
    is( $err, undef, "failed insert with w => 0 does not throw an error" );

    $err = exception { $eve_coll->insert_one( { _id => 'foo' } ) };
    like($err->message, qr/not authorized/, "failed insert with default w throws exception" );
};

subtest "safe and unsafe update" => sub {
    $alice_coll->drop;

    my $err = exception { $unsafe_eve_coll->update_one( { _id => 'foo' }, { '$inc' => { count => 1 } }, { upsert => 1 } ) };
    is( $err, undef, "failed update with w => 0 does not throw an error" );

    $err = exception { $eve_coll->insert_one( { _id => 'foo' } ) };
    like($err->message, qr/not authorized/, "failed insert with default w throws exception" );
};

clear_testdbs;

done_testing;
