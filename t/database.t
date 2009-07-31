use strict;
use warnings;
use Test::More tests => 11;
use Test::Exception;

use MongoDB;

my $conn = MongoDB::Connection->new;
isa_ok($conn, 'MongoDB::Connection');

my $db = $conn->get_database('test_database');
isa_ok($db, 'MongoDB::Database');

$db->drop;

is(scalar $db->collection_names, 0, 'no collections');
my $coll = $db->get_collection('test');
is($coll->count, 0, 'collection is empty');

is($coll->find_one, undef, 'nothing for find_one');

my $id = $coll->insert({ just => 'another', perl => 'hacker' });

is(scalar $db->collection_names, 3, 'test, system.indexes, and test.$_id_');
ok((grep { $_ eq 'test' } $db->collection_names), 'collection_names');
is($coll->count, 1, 'count');
is($coll->find_one->{perl}, 'hacker', 'find_one');
is($coll->find_one->{_id}->value, $id->value, 'insert id');

is($db->run_command({ foo => 'bar' }), "no such cmd");

END {
    $db->drop;
}
