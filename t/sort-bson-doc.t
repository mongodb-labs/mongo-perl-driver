#  Copyright 2015 - present MongoDB, Inc.
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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;

use MongoDB;
use MongoDB::Error;
use BSON::Types ':all';

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type get_capped/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

$coll->insert_many( [
  { _id => 1, size => 10 },
  { _id => 2, size => 5  },
  { _id => 3, size => 15 },
] );

subtest "sort standard hash" => sub {
  my @res = $coll->find( {}, { sort => { size => 1 } } )->result->all;

  cmp_deeply \@res,
    [
      { _id => 2, size => 5  },
      { _id => 1, size => 10 },
      { _id => 3, size => 15 },
    ],
    'Got correct sort order';
};

subtest "sort BSON::Doc" => sub {
  my $b_doc = bson_doc( size => 1 );
  my @res = $coll->find( {}, { sort => $b_doc } )->result->all;

  cmp_deeply \@res,
    [
      { _id => 2, size => 5  },
      { _id => 1, size => 10 },
      { _id => 3, size => 15 },
    ],
    'Got correct sort order';
};

done_testing;
