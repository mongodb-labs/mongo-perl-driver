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
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db/;

note("CAP-408 aggregation explain");

my $orc =
    MongoDBTest::Orchestrator->new( config_file => "devel/t-dynamic/sharded-2.4-mixed.yml" );
diag "starting cluster";
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

my $conn   = build_client( dt_type => undef );
my $admin  = $conn->get_database("admin");
my $testdb = get_test_db($conn);
my $coll   = $testdb->get_collection("test_collection");

$admin->_try_run_command([enableSharding => $testdb->name]);
$admin->_try_run_command([shardCollection => $coll->full_name, key => { number => 1 }]);

$coll->insert( { number => int(rand(2**31)) } ) for 1 .. 10;

like(
    exception { $coll->aggregate( [ { '$project' => { _id => 1, count => 1 } } ], {explain => 1} ) },
    qr/pipeline/,
    "caught exception running explain on mixed cluster"
);

done_testing;
