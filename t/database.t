use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;

use MongoDB;

my $conn = MongoDB::Connection->new;
isa_ok($conn, 'MongoDB::Connection');

my $db = $conn->get_database('test_database');
isa_ok($db, 'MongoDB::Database');

$db->drop;

my $coll = $db->get_collection('test');
my $id   = $coll->insert({ just => 'another', perl => 'hacker' });

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
