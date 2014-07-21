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
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use Log::Any::Adapter qw/Stderr/;

use MongoDBTest::Orchestrator;

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/clusters/mongod-2.6-auth.yml" );
diag "starting server with auth enabled";
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client( dt_type => undef );
my $admin  = $conn->get_database("admin");
my $testdb = get_test_db($conn);
my $coll   = $testdb->get_collection("test_collection");

note("QA-447 MIXED OPERATIONS, AUTH");

is( exception { $coll->drop }, undef, "drop collection" );

# create limited role and user
$testdb->_try_run_command(
    [
        createRole => 'CRU',
        privileges => [
            {
                resource => { db => $testdb->name, collection => '' },
                actions  => [qw/find insert update/],
            },
        ],
        roles => [],
    ]
);

$testdb->_try_run_command(
    [ createUser => "limited", pwd => "limited", roles => [ "CRU" ] ] );

local $ENV{MONGOD} = $ENV{MONGOD};
$ENV{MONGOD} =~ s{mongodb://.*?\@}{mongodb://};
my $limited = build_client( db_name => $testdb->name, username => 'limited', password => 'limited' );
my $coll2 = $limited->get_database($testdb->name)->get_collection($coll->name);
my $bulk = $coll2->initialize_ordered_bulk_op;
$bulk->insert( { _id => 1 } );
$bulk->find( { _id => 1 } )->remove( { _id => 1 } );

my ( $result, $err );
$err = exception { $result = $bulk->execute };
like( $err->message, qr/not authorized/, "no error on bulk op by limited user" ) or diag explain $err;

is( $coll2->find({})->count, 1, "document inserted but not removed"); 

done_testing;
