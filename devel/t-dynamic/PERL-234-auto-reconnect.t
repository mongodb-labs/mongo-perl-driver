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

use lib "t/lib";
use lib "devel/lib";

use MongoDBTest::Orchestrator;
# use Log::Any::Adapter qw/Stderr/;

my $orc = MongoDBTest::Orchestrator->new( config_file => "devel/clusters/mongod-2.6.yml" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;

use MongoDBTest qw/build_client get_test_db/;

my $server = $orc->cluster->get_server("host1");
my $orig_port = $server->port;

is ($server->is_alive, 1, "Server is alive");

my $c = build_client();
my $coll = get_test_db($c)->get_collection("testc");
inserted_ok($coll, $coll->insert({pre => 'stop'}));

$server->stop();

is ($server->is_alive, undef, "Server is dead");

$server->start($orig_port);	
inserted_ok($coll, $coll->insert({post => 'reconnect'}));

done_testing;

sub inserted_ok {

    my ($coll, $id) = @_;

    ok($coll->find({_id => $id}), "$id inserted (find)");
    ok($coll->find_one({_id => $id}), "$id inserted (find_one)");
}