#  Copyright 2016 - present MongoDB, Inc.
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
use Test::Fatal;
use Tie::IxHash;
use version;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type/;

$ENV{PERL_MONGO_NO_DEP_WARNINGS} = 1;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);

my $coll = $testdb->get_collection('test_collection');

my $cursor;
my @values;

# test setup
{
    $coll->drop;

    $coll->insert_one({ foo => 9,  bar => 3, shazbot => 1 });
    $coll->insert_one({ foo => 2,  bar => 5 });
    $coll->insert_one({ foo => -3, bar => 4 });
    $coll->insert_one({ foo => 4,  bar => 9, shazbot => 1 });
}

# snapshot
# XXX tests don't fail if snapshot is turned off ?!?
subtest "snapshot" => sub {
    plan skip_all => "Snapshot removed in 3.7+"
      unless $server_version < v3.7.0;

    my $cursor3 = $coll->query->snapshot(1);
    is( $cursor3->has_next, 1, 'check has_next' );
    my $r1 = $cursor3->next;
    is( $cursor3->has_next, 1,
        'if this failed, the database you\'re running is old and snapshot won\'t work' );
    $cursor3->next;
    is( int $cursor3->has_next, 0, 'check has_next is false' );

    like(
        exception { $coll->query->snapshot },
        qr/requires a defined, boolean argument/,
        "snapshot exception without argument"
    );
};

done_testing;
