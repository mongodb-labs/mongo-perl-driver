use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;

use MongoDB;

my $conn = MongoDB::Connection->new;
my $db = $conn->get_database('test_database');
$db->drop;

my $coll = $db->get_collection('test_collection');

is($coll->query->next, undef);
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
