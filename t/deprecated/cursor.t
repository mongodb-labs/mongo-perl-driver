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
my $coll2 = $testdb->get_collection("cap_collection");

# after dropping coll2, must run command below to make it capped
my $create_capped_cmd = [ create => "cap_collection", capped => 1, size => 10000 ];

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

# count
{
    $coll->drop;
    is ($coll->count, 0, "empty" );
    $coll->insert_many([{'x' => 1}, {'x' => 1}, {'y' => 1}, {'x' => 1, 'z' => 1}]);

    is($coll->query->count, 4, 'count');
    is($coll->query({'x' => 1})->count, 3, 'count query');

    is($coll->query->limit(1)->count(1), 1, 'count limit');
    is($coll->query->skip(1)->count(1), 3, 'count skip');
    is($coll->query->limit(1)->skip(1)->count(1), 1, 'count limit & skip');
}

# cursor opts
# not a functional test, just make sure they don't blow up
{
    $cursor = $coll->find();

    $cursor->slave_okay(1);
    is($cursor->_query->read_preference->mode, 'secondaryPreferred', "set slave_ok");
    $cursor->slave_okay(0);
    is($cursor->_query->read_preference->mode, 'primary', "clear slave_ok");
}

subtest "count w/ hint" => sub {

    $coll->drop;
    $coll->insert_one( { i => 1 } );
    $coll->insert_one( { i => 2 } );
    is ($coll->find()->count(), 2, 'count = 2');

    $coll->indexes->create_one( { i => 1 } );

    is( $coll->find( { i => 1 } )->hint( '_id_' )->count(), 1, 'count w/ hint & spec');
    is( $coll->find()->hint( '_id_' )->count(), 2, 'count w/ hint');

    my $current_version = version->parse($server_version);
    my $version_2_6 = version->parse('v2.6');

    if ( $current_version > $version_2_6 ) {

        eval { $coll->find( { i => 1 } )->hint( 'BAD HINT')->count() };
        like($@, ($server_type eq "Mongos" ? qr/failed/ : qr/bad hint/ ), 'check bad hint error');

    } else {

        is( $coll->find( { i => 1 } )->hint( 'BAD HINT' )->count(), 1, 'bad hint and spec');
    }

    $coll->indexes->create_one( { x => 1 }, { sparse => 1 } );

    if ($current_version > $version_2_6 ) {

        is( $coll->find( {  i => 1 } )->hint( 'x_1' )->count(), 0, 'spec & hint on empty sparse index');

    } else {

        is( $coll->find( {  i => 1 } )->hint( 'x_1' )->count(), 1, 'spec & hint on empty sparse index');
    }

    # XXX Failing on nightly master -- xdg, 2016-02-11
    TODO: {
        local $TODO = "Failing nightly master";
        is( $coll->find()->hint( 'x_1' )->count(), 2, 'hint on empty sparse index');
    }
};

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
