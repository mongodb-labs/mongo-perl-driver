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

use strict;
use warnings;
use Test::More;
use MongoDB;
use boolean;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

plan skip_all => "Requires Test::Memory::Cycle"
  unless eval { require Test::Memory::Cycle; 1 };

my $client = build_client();
my $testdb = get_test_db($client);
my $coll = $testdb->coll("testtesttest");

$coll->insert_one({ a => false }) for 1 .. 100;
my @docs = $coll->find({})->all;

Test::Memory::Cycle::memory_cycle_ok( $client );

done_testing;
# COPYRIGHT

# vim: set ts=4 sts=4 sw=4 et tw=75:
