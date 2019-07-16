#  Copyright 2019 - present MongoDB, Inc.
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
use JSON::MaybeXS qw( is_bool decode_json );
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.96;
use Test::Deep;
use Math::BigInt;
use Storable qw( dclone );

use utf8;

use MongoDB;
use MongoDB::_Types qw/
    to_IxHash
/;
use MongoDB::Error;

use lib "t/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
    skip_unless_mongod
    skip_unless_transactions
    skip_unless_min_version
/;

skip_unless_mongod();
skip_unless_transactions();

my $conn           = build_client();
my $server_version = server_version($conn);
my $server_type    = server_type($conn);

plan skip_all => "test is for mongos only"
    unless $conn->_topology->type eq 'Sharded';

skip_unless_min_version($conn, 'v4.1.6');

plan skip_all => "test deployment must have multiple named mongos"
    if scalar( $conn->_topology->all_servers ) < 2;

my $test_db = get_test_db($conn);

subtest 'Starting new transaction unpins client session' => sub {
    my $session = $conn->start_session;
    my $collection = get_unique_collection($test_db, 'mongos-pinning-with');
    # make sure the collection is actually created
    $test_db->run_command([ create => $collection->name ]);

    $session->start_transaction;
    $collection->insert_one({}, { session => $session });

    $session->commit_transaction;

    my %addresses;

    for ( 0 .. 20 ) {
      $session->start_transaction;
      my $cursor = $collection->find({}, { session => $session });
      my $tmp = $cursor->next;

      $addresses{ $cursor->result->_address }++;

      $session->commit_transaction;
    }

    ok scalar( keys %addresses ) > 1, 'got more than one address for a sharded cluster';

    $collection->drop;
};

subtest 'Non transactions operations unpin session' => sub {
    my $session = $conn->start_session;
    my $collection = get_unique_collection($test_db, 'mongos-pinning-non');
    # make sure the collection is actually created
    $test_db->run_command([ create => $collection->name ]);

    $session->start_transaction;
    $collection->insert_one({}, { session => $session });

    $session->commit_transaction;

    my %addresses;

    for ( 0 .. 20 ) {
      my $cursor = $collection->find({}, { session => $session });
      my $tmp = $cursor->next;

      $addresses{ $cursor->result->_address }++;
    }

    ok scalar( keys %addresses ) > 1, 'got more than one address for a sharded cluster';

    $collection->drop;
};

clear_testdbs;

done_testing;
