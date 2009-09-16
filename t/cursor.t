use strict;
use warnings;
use Test::More tests => 49;
use Test::Exception;

use MongoDB;

my $conn = MongoDB::Connection->new;
my $db = $conn->get_database('test_database');
$db->drop;

my $coll = $db->get_collection('test_collection');

$coll->insert({ foo => 9,  bar => 3, shazbot => 1 });
$coll->insert({ foo => 2,  bar => 5 });
$coll->insert({ foo => -3, bar => 4 });
$coll->insert({ foo => 4,  bar => 9, shazbot => 1 });

my @values;

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

# criteria
@values = $coll->query({ shazbot => 1 }, { sort_by => { foo => -1 } })->all;
is(scalar @values, 2);
is($values[0]->{foo}, 9);
is($values[1]->{foo}, 4);

# limit
@values = $coll->query({}, { limit => 3, sort_by => { foo => 1 } })->all;
is(scalar @values, 3);
is ($values[0]->{foo}, -3);
is ($values[1]->{foo}, 2);
is ($values[2]->{foo}, 4);

# skip
@values = $coll->query({}, { limit => 3, skip => 1, sort_by => { foo => 1 } })->all;
is(scalar @values, 3);
is ($values[0]->{foo}, 2);
is ($values[1]->{foo}, 4);
is ($values[2]->{foo}, 9);

$db->drop;

$coll = $db->get_collection('test_collection');

is($coll->query->next, undef, 'test undef');
is_deeply([$coll->query->all], []);

my $id1 = $coll->insert({x => 1});
my $id2 = $coll->insert({x => 5});

is($coll->count, 2);
my $cursor = $coll->query;
is($cursor->next->{'x'}, 1);
is($cursor->next->{'x'}, 5);
is($cursor->next, undef);

my $cursor2 = $coll->query({x => 5});
is_deeply([$cursor2->all], [{_id => $id2, x => 5}]);

is_deeply([$coll->query->all], [{_id => $id1, x => 1}, {_id => $id2, x => 5}]);

my $cursor_sort = $coll->query->sort({'x' => -1});
is($cursor_sort->has_next, 1);
is($cursor_sort->next->{'x'}, 5, 'Cursor->sort');
is($cursor_sort->next->{'x'}, 1);

$cursor_sort = $coll->query->sort({'x' => 1});
is($cursor_sort->next->{'x'}, 1);
is($cursor_sort->next->{'x'}, 5);

my $cursor3 = $coll->query->snapshot;
is($cursor3->has_next, 1, 'check has_next');
my $r1 = $cursor3->next;
is($cursor3->has_next, 1, 'if this failed, the database you\'re running is old and snapshot won\'t work');
$cursor3->next;
is(int $cursor3->has_next, 0, 'check has_next is false');

$coll->insert({x => 2});
$coll->insert({x => 3});
$coll->insert({x => 4});
my $paging = $coll->query->skip(1)->limit(2);
is($paging->has_next, 1, 'check skip/limit');
$paging->next;
is($paging->has_next, 1);
$paging->next;
is(int $paging->has_next, 0);


my $collection = $db->get_collection('test');
$collection->drop;
$collection->ensure_index(['sn']);

my $sn = 0;
while ($sn <= 500) {
  $collection->insert({sn => $sn++});
}

$cursor = $collection->query();
my $count = 0;
while (my $doc = $cursor->next()) {
    $count++;
}
is(501, $count);

# reset
$cursor->reset;
$r1 = $cursor->next;
$cursor->reset;
my $r2 = $cursor->next;

is($r1->{'sn'}, $r2->{'sn'}, 'reset');

# explain
my $exp = $cursor->explain;
is(501, $exp->{'n'}, 'explain');
is('BasicCursor', $exp->{'cursor'});
ok(exists $exp->{'endKey'});

$cursor->reset;
$exp = $cursor->limit(20)->explain;
is(20, $exp->{'n'}, 'explain limit');
$cursor->reset;
$exp = $cursor->limit(-20)->explain;
is(20, $exp->{'n'});

#hint
$cursor->reset;
my $hinted = $cursor->hint({'x' => 1});
is($hinted, $cursor);

$collection->drop;

$collection->insert({'num' => 1, 'foo' => 1});

my $aok = 1;
eval {
    $collection->query->hint({'num' => 1})->explain;
    $aok = 0;
};

ok($@ =~ m/^bad hint at/);

