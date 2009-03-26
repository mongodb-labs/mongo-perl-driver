use strict;
use warnings;
use Test::More tests => 10;
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

# TODO enable once fixed
# is($coll->find_one, undef, 'nothing for find_one');

my $id = $coll->insert({ just => 'another', perl => 'hacker' });

is(scalar $db->collection_names, 1, 'one collection');
ok((grep { $_ eq 'test' } $db->collection_names), 'collection_names');
is($coll->count, 1, 'count');
is($coll->find_one->{perl}, 'hacker', 'find_one');
is($coll->find_one->{_id}->value, $id->value, 'insert id');

throws_ok {
    $db->run_command({ foo => 'bar' });
} qr/no such cmd/;

END {
    $db->drop;
}
