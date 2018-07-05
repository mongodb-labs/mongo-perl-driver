#  Copyright 2018 - present MongoDB, Inc.
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
use Test::More;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    clear_testdbs
/;
use MongoDBTest::Callback;

skip_unless_mongod();

my $cb = MongoDBTest::Callback->new;
my $conn = build_client(monitoring_callback => $cb->callback);
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $coll = $testdb->get_collection('test_collection');

plan skip_all => 'MongoDB version 3.6 or higher required for OP_MSG support'
    unless $server_version >= version->parse('v3.6.0');

subtest 'insert document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->insert_one([ _id => 1 ]);
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is $ret->inserted_id, 1, 'Correct inserted id';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ { _id => 1 } ], 'Collection info correct';
  $coll->drop;
};

clear_testdbs;

done_testing;
