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
use Test::More 0.88;
use Test::Fatal;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;

use MongoDBTest qw/build_client get_test_db clear_testdbs server_version
  server_type skip_if_mongod skip_unless_mongod/;

# This test starts servers on localhost ports 27017, 27018 and 27019. We skip if
# these aren't available.
for my $port ( 27017, 27018, 27019 ) {
    local $ENV{MONGOD} = "mongodb://localhost:$port/";
    skip_if_mongod();
}

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/replicaset-any-27017.yml" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;

my $client         = build_client( server_selection_timeout_ms => 5000 );
my $testdb         = get_test_db($client);
my $server_version = server_version($client);
my $server_type    = server_type($client);
my $coll           = $testdb->get_collection('test_collection');

subtest "Add tests here" => sub {
    ok( $coll->insert_one( {} ), "We can insert" );
};

clear_testdbs;
done_testing;
