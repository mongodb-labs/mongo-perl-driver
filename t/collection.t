use strict;
use warnings;
use Test::More tests => 43;
use Test::Exception;

use Tie::IxHash;

use MongoDB;

my $conn = MongoDB::Connection->new;
my $db   = $conn->get_database('test_database');
my $coll = $db->get_collection('test_collection');
isa_ok($coll, 'MongoDB::Collection');

is($coll->name, 'test_collection', 'get name');

$db->drop;

my $id = $coll->insert({ just => 'another', perl => 'hacker' });
is($coll->count, 1, 'count');

$coll->update({ _id => $id }, {
    just => "an\xE4oth\0er",
    mongo => 'hacker',
    with => { a => 'reference' },
    and => [qw/an array reference/],
});
is($coll->count, 1);

is($coll->count({ mongo => 'programmer' }), 0, 'count = 0');
is($coll->count({ mongo => 'hacker'     }), 1, 'count = 1');
is($coll->count({ 'with.a' => 'reference' }), 1, 'inner obj count');

my $obj = $coll->find_one;
is($obj->{mongo} => 'hacker', 'find_one');
is(ref $obj->{with}, 'HASH', 'find_one type');
is($obj->{with}->{a}, 'reference');
is(ref $obj->{and}, 'ARRAY');
is_deeply($obj->{and}, [qw/an array reference/]);
ok(!exists $obj->{perl});
is($obj->{just}, "an\xE4oth\0er");

lives_ok {
    $coll->validate;
} 'validate';

$coll->remove($obj);
is($coll->count, 0, 'remove() deleted everything (won\'t work on an old version of Mongo)');

$coll->drop;
for (my $i=0; $i<10; $i++) {
    $coll->insert({'x' => $i, 'z' => 3, 'w' => 4});
    $coll->insert({'x' => $i, 'y' => 2, 'z' => 3, 'w' => 4});
}
is($coll->count({}, {y => 1}), 10, 'count fields');

$coll->drop;
ok(!$coll->get_indexes, 'no indexes yet');

$coll->ensure_index([qw/foo bar baz/]);
$coll->ensure_index([qw/foo bar/]);
$coll->insert({foo => 1, bar => 1, baz => 1, boo => 1});
$coll->insert({foo => 1, bar => 1, baz => 1, boo => 2});
is($coll->count, 2);

$coll->ensure_index([qw/boo/], "ascending", 1);
$coll->insert({foo => 3, bar => 3, baz => 3, boo => 2});

is($coll->count, 2, 'unique index');

my @indexes = $coll->get_indexes;
is(scalar @indexes, 4, 'three custom indexes and the default _id_ index');
is_deeply(
    [sort keys %{ $indexes[1]->{key} }],
    [sort qw/foo bar baz/],
);
is_deeply(
    [sort keys %{ $indexes[2]->{key} }],
    [sort qw/foo bar/],
);

$coll->drop_index($indexes[1]->{name});
@indexes = $coll->get_indexes;
is(scalar @indexes, 3);
is_deeply(
    [sort keys %{ $indexes[1]->{key} }],
    [sort qw/foo bar/],
);

$coll->drop;
ok(!$coll->get_indexes, 'no indexes after dropping');

# test doubles
my $pi = 3.14159265;
ok($id = $coll->insert({ data => 'pi', pi => $pi }), "inserting float number value");
ok($obj = $coll->find_one({ data => 'pi' }));
is($obj->{pi}, $pi);

# test undefined values
ok($id  = $coll->insert({ data => 'null', none => undef }), 'inserting undefined data');
ok($obj = $coll->find_one({ data => 'null' }), 'finding undefined row');
ok(exists $obj->{none}, 'got null field');
ok(!defined $obj->{none}, 'null field is undefined');

$coll->drop;

# ord("\x9F") is 159
$coll->insert({foo => "\x9F" });
my $utfblah = $coll->find_one;
is(ord($utfblah->{'foo'}), 194, 'translate non-utf8 to utf8 char');

$coll->drop;
$coll->insert({"\x9F" => "hi"});
$utfblah = $coll->find_one;
is($utfblah->{chr(159)}, "hi", 'translate non-utf8 key');


$coll->drop;
my $keys = tie(my %idx, 'Tie::IxHash');
%idx = ('sn' => 'ascending', 'ts' => 'descending');

$coll->ensure_index($keys);

my @tied = $coll->get_indexes;
is(scalar @tied, 2, 'num indexes');
is($tied[1]->{'ns'}, 'test_database.test_collection', 'namespace');
is($tied[1]->{'name'}, 'sn_1_ts_-1', 'namespace');

$coll->drop;

$coll->insert({x => 1, y => 2, z => 3, w => 4});
my $cursor = $coll->query->fields({'y' => 1});
$obj = $cursor->next;
is(exists $obj->{'y'}, 1, 'y exists');
is(exists $obj->{'_id'}, 1, '_id exists');
is(exists $obj->{'x'}, '', 'x doesn\'t exist');
is(exists $obj->{'z'}, '', 'z doesn\'t exist');
is(exists $obj->{'w'}, '', 'w doesn\'t exist');


END {
    $db->drop;
}
