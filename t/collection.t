use strict;
use warnings;
use Test::More tests => 7;
use Test::Exception;

use Mongo;

my $conn = Mongo::Connection->new;
my $db   = $conn->get_database('test_database');
my $coll = $db->get_collection('test_collection');
isa_ok($coll, 'Mongo::Collection');

is($coll->name, 'test_collection', 'get name');

$db->drop;

my $id = $coll->insert({ just => 'another', perl => 'hacker' });
is($coll->count, 1);

$coll->update({ _id => $id }, { just => 'another', mongo => 'hacker' });
is($coll->count, 1);

my $obj = $coll->find_one;
is($obj->{mongo} => 'hacker');
ok(!exists $obj->{perl});

$coll->remove($obj);
is($coll->count, 0);

END {
    $db->drop;
}
