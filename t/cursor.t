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
use Tie::IxHash;
use version;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_version server_type/;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);

my $coll;
my $cursor;
my @values;

$testdb->drop;

# test setup
{
    $coll = $testdb->get_collection('test_collection');

    $coll->insert({ foo => 9,  bar => 3, shazbot => 1 });
    $coll->insert({ foo => 2,  bar => 5 });
    $coll->insert({ foo => -3, bar => 4 });
    $coll->insert({ foo => 4,  bar => 9, shazbot => 1 });
}

# $coll->query
{
    @values = $coll->query({}, { sort_by => { foo => 1 } })->all;

    is(scalar @values, 4);
    is ($values[0]->{foo}, -3);
    is ($values[1]->{foo}, 2);
    is ($values[2]->{foo}, 4);
    is ($values[3]->{foo}, 9);

    @values = $coll->query({}, { sort_by => { bar => -1 } })->all;

    is(scalar @values, 4);
    is($values[0]->{bar}, 9);
    is($values[1]->{bar}, 5);
    is($values[2]->{bar}, 4);
    is($values[3]->{bar}, 3);
}

# criteria
{
    @values = $coll->query({ shazbot => 1 }, { sort_by => { foo => -1 } })->all;
    is(scalar @values, 2);
    is($values[0]->{foo}, 9);
    is($values[1]->{foo}, 4);
}

# limit
{
    @values = $coll->query({}, { limit => 3, sort_by => { foo => 1 } })->all;
    is(scalar @values, 3) or diag explain \@values;
    is ($values[0]->{foo}, -3);
    is ($values[1]->{foo}, 2);
    is ($values[2]->{foo}, 4);
}

# skip
{
    @values = $coll->query({}, { limit => 3, skip => 1, sort_by => { foo => 1 } })->all;
    is(scalar @values, 3);
    is ($values[0]->{foo}, 2);
    is ($values[1]->{foo}, 4);
    is ($values[2]->{foo}, 9);
}

$testdb->drop;

# next and all
{
    $coll = $testdb->get_collection('test_collection');

    is($coll->query->next, undef, 'test undef');
    is_deeply([$coll->query->all], []);

    my $id1 = $coll->insert({x => 1});
    my $id2 = $coll->insert({x => 5});

    is($coll->count, 2);
    $cursor = $coll->query;
    is($cursor->next->{'x'}, 1);
    is($cursor->next->{'x'}, 5);
    is($cursor->next, undef);

    my $cursor2 = $coll->query({x => 5});
    is_deeply([$cursor2->all], [{_id => $id2, x => 5}]);

    is_deeply([$coll->query->all], [{_id => $id1, x => 1}, {_id => $id2, x => 5}]);
}

# sort, and sort by tie::ixhash
{
    my $cursor_sort = $coll->query->sort({'x' => -1});
    is($cursor_sort->has_next, 1);
    is($cursor_sort->next->{'x'}, 5, 'Cursor->sort');
    is($cursor_sort->next->{'x'}, 1);

    $cursor_sort = $coll->query->sort({'x' => 1});
    is($cursor_sort->next->{'x'}, 1);
    is($cursor_sort->next->{'x'}, 5);
    
    my $hash = Tie::IxHash->new("x" => -1);
    $cursor_sort = $coll->query->sort($hash);
    is($cursor_sort->has_next, 1);
    is($cursor_sort->next->{'x'}, 5, 'Tie::IxHash cursor->sort');
    is($cursor_sort->next->{'x'}, 1);
}

# snapshot
{
    my $cursor3 = $coll->query->snapshot;
    is($cursor3->has_next, 1, 'check has_next');
    my $r1 = $cursor3->next;
    is($cursor3->has_next, 1, 'if this failed, the database you\'re running is old and snapshot won\'t work');
    $cursor3->next;
    is(int $cursor3->has_next, 0, 'check has_next is false');
}

# paging
{
    $coll->insert({x => 2});
    $coll->insert({x => 3});
    $coll->insert({x => 4});
    my $paging = $coll->query->skip(1)->limit(2);
    is($paging->has_next, 1, 'check skip/limit');
    $paging->next;
    is($paging->has_next, 1);
    $paging->next;
    is(int $paging->has_next, 0);
}

# bigger test, with index
{
    $coll = $testdb->get_collection('test');
    $coll->drop;
    $coll->ensure_index({'sn'=>1});

    my $bulk = $coll->unordered_bulk;
    $bulk->insert({sn => $_}) for 0 .. 5000;
    $bulk->execute;

    $cursor = $coll->query;
    my $count = 0;
    while (my $doc = $cursor->next()) {
        $count++;
    }
    is(5001, $count);

    my @all = $coll->find->limit(3999)->all;
    is( 0+@all, 3999, "got limited documents" );
}

# reset
{
    $cursor->reset;
    my $r1 = $cursor->next;
    $cursor->reset;
    my $r2 = $cursor->next;

    is($r1->{'sn'}, $r2->{'sn'}, 'reset');
}

# explain
{
    my $exp = $cursor->explain;
    is($exp->{'n'}, 5001, 'explain');
    is($exp->{'cursor'}, 'BasicCursor');

    $cursor->reset;
    $exp = $cursor->limit(20)->explain;
    is(20, $exp->{'n'}, 'explain limit');
    $cursor->reset;
    $exp = $cursor->limit(-20)->explain;
    is(20, $exp->{'n'});
}

# hint
{
    $cursor->reset;
    my $hinted = $cursor->hint({'x' => 1});
    is($hinted, $cursor);

    $coll->drop;

    $coll->insert({'num' => 1, 'foo' => 1});

    my $aok = 1;
    eval {
        $coll->query->hint({'num' => 1})->explain;
        $aok = 0;
    };

    like($@, qr/query error/, "check query error on hint");
}

# slave_okay
{
    # MongoDB::Cursor::slave_okay
    $MongoDB::Cursor::slave_okay = 1;
    $cursor = $coll->query->next;

    $MongoDB::Cursor::slave_okay = 0;
    $cursor = $coll->query->next;

    $coll->drop;
}

# count
{
    $coll->drop;
    $coll->batch_insert([{'x' => 1}, {'x' => 1}, {'y' => 1}, {'x' => 1, 'z' => 1}]);

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

	$cursor = $cursor->tailable(1);
	is($cursor->_tailable, 1);
	$cursor = $cursor->tailable(0);
	is($cursor->_tailable, 0);

    $cursor = $coll->find()->tailable(1);
    is($cursor->_tailable, 1);
    $cursor = $coll->find()->tailable(0);
    is($cursor->_tailable, 0);
    
    #test is actual cursor
    $coll->drop;
    $coll->insert({"x" => 1});
    $cursor = $coll->find()->tailable(0);
    my $doc = $cursor->next;
    is($doc->{'x'}, 1);
    
	$cursor = $coll->find();

    $cursor->immortal(1);
    is($cursor->immortal, 1);
    $cursor->immortal(0);
    is($cursor->immortal, 0);

    $cursor->slave_okay(1);
    is($cursor->slave_okay, 1);
    $cursor->slave_okay(0);
    is($cursor->slave_okay, 0);
}

# explain
{
    $coll->drop;

    $coll->insert({"x" => 1});

    $cursor = $coll->find;
    my $doc = $cursor->next;
    is($doc->{'x'}, 1);

    my $exp = $cursor->explain;

    # cursor should be reset
    $doc = $cursor->next;
    is($doc->{'x'}, 1);
}

# info
{
    $cursor = $coll->find;
    my $count = $coll->count;

    my $info = $cursor->info;
    is($info->{'num'}, 0);

    $cursor->has_next;
    $info = $cursor->info;
    is($info->{'at'}, 0);
    is($info->{'num'}, $count);
    is($info->{'start'}, 0);
    is($info->{'cursor_id'}, scalar("\0" x 8));

    $cursor->next;
    $info = $cursor->info;
    is($info->{'at'}, 1);
}

# sort_by
{
    $coll->drop;

    for (my $i=0; $i < 5; $i++) {
        $coll->insert({x => $i});
    }

    $cursor = $testdb->get_collection( 'test_collection' )->query({}, { limit => 10, skip => 0, sort_by => {created => 1 }});
    is($cursor->count(), 5);
}

# delayed tailable cursor
{
    $coll = $testdb->get_collection( 'test_collection' );
    $coll->drop;

    my $cmd = [ create => "test_collection", capped => 1, size => 10000 ];
    $testdb->run_command($cmd);

    $coll->insert( { x => $_ } ) for 0 .. 9;

    # Get last doc
    my $cursor = $coll->find()->sort({x => -1})->limit(1);
    my $last_doc = $cursor->next();

    $cursor = $coll->find({_id => {'$gt' => $last_doc->{_id}}})->tailable(1);

    # We won't get anything yet
    $cursor->next();

    for (my $i=10; $i < 20; $i++) {
        $coll->insert({x => $i});
    }

    # We should retrieve documents here since we are tailable.
    my $count =()= $cursor->all;

    is($count, 10);
}

subtest "count w/ hint" => sub {

    $coll->drop;
    $coll->insert( { i => 1 } );
    $coll->insert( { i => 2 } );
    is ($coll->find()->count(), 2, 'count = 2');

    $coll->ensure_index( { i => 1 } );

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

    $coll->ensure_index( { x => 1 }, { sparse => 1 } );

    if ($current_version > $version_2_6 ) {

        is( $coll->find( {  i => 1 } )->hint( 'x_1' )->count(), 0, 'spec & hint on empty sparse index');

    } else {

        is( $coll->find( {  i => 1 } )->hint( 'x_1' )->count(), 1, 'spec & hint on empty sparse index');
    }

    is( $coll->find()->hint( 'x_1' )->count(), 2, 'hint on empty sparse index');
};

done_testing;
