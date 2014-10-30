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

# use Log::Any::Adapter qw/Stderr/;

use MongoDBTest::Orchestrator;

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/replicaset-any.yml" );
diag "starting replicaset";
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

use MongoDBTest qw/build_client get_test_db clear_testdbs/;

my $conn = build_client( dt_type => undef );
my $admin  = $conn->get_database("admin");
my $testdb = get_test_db($conn);
my $coll   = $testdb->get_collection("test_collection");

subtest "connect to RS without primary" => sub {
    diag "waiting for all hosts to be ready";
    $orc->deployment->server_set->wait_for_all_hosts;

    is( exception { $coll->drop }, undef, "drop collection" );

    $coll->insert( {} );

    # stepdown primary
    $orc->deployment->server_set->stepdown_primary(5);
    note "stepped down primary";

    my $conn2 = build_client( dt_type => undef );
    $conn2->read_preference('primary_preferred');
    my $coll2 = $conn2->get_database($testdb->name)->get_collection("test_collection");
    my $count = $coll2->count;

    is( $count, 1, "read count from secondary" );
};

clear_testdbs;

done_testing;
