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
    skip_unless_min_version
/;
use MongoDBTest::Callback;

skip_unless_mongod();

my $conn = build_client();
my $server_version = server_version($conn);

skip_unless_min_version($conn, 'v3.6.0');

# Reconstruct client with monitoring on.  Doing this after the skip to try to
# avoid rare test crashes on global destruction on Perl 5.18 with threads.
my $cb = MongoDBTest::Callback->new;
$conn = build_client(monitoring_callback => $cb->callback);
my $testdb = get_test_db($conn);
my $coll = $testdb->get_collection('test_collection');

subtest 'insert single document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->insert_one([ _id => 1 ]);
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is $ret->inserted_id, 1, 'Correct inserted id';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ { _id => 1 } ], 'Collection info correct';
};

subtest 'insert multiple document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->insert_many([[ _id => 2 ], [ _id => 3 ]]);
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is_deeply $ret->inserted_ids, { 0 => 2, 1 => 3 }, 'Correct inserted id';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ { _id => 1 }, { _id => 2 }, { _id => 3 } ], 'Collection info correct';
};

subtest 'update single document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->update_one({ _id => 1 }, { '$set' => { eg => 2 } });
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is $ret->modified_count, 1, 'Correct modified count';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ { _id => 1, eg => 2 }, { _id => 2 }, { _id => 3 } ], 'Collection info correct';
};

subtest 'update multiple document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->update_many({ _id => { '$gte' => 2 } }, { '$set' => { eg => 3 } });
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is $ret->modified_count, 2, 'Correct modified count';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ { _id => 1, eg => 2 }, { _id => 2, eg => 3 }, { _id => 3, eg => 3 } ], 'Collection info correct';
};

subtest 'delete single document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->delete_one([ _id => 1 ]);
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is $ret->deleted_count, 1, 'Correct deleted count';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ { _id => 2, eg => 3 }, { _id => 3, eg => 3 } ], 'Collection info correct';
};

subtest 'delete multiple document' => sub {
  $cb->clear_events;
  $ENV{DO_OP_MSG} = 1;
  my $ret = $coll->delete_many([ _id => { '$gte' => 2 } ]);
  $ENV{DO_OP_MSG} = 0;

  # OP_MSG enforces $db to be in the command itself
  is $cb->events->[-2]{command}{'$db'}, $testdb->name, 'Sent to correct database';
  is $ret->deleted_count, 2, 'Correct deleted count';

  my @collection = $coll->find()->all;

  is_deeply \@collection, [ ], 'Collection info correct';
};

clear_testdbs;

done_testing;
