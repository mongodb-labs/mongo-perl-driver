use strict;
use warnings;
use Test::More;
use Test::Exception;

use Data::Types qw(:float);
use Tie::IxHash;

use MongoDB;

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::Connection->new(host => $host);
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 113;
}

my $db   = $conn->get_database('test_database');
$db->drop;

my $coll = $db->get_collection('test_collection');
isa_ok($coll, 'MongoDB::Collection');

is($coll->name, 'test_collection', 'get name');

$db->drop;

# very small insert
my $id = $coll->insert({_id => 1});
is($id, 1);
my $tiny = $coll->find_one;
is($tiny->{'_id'}, 1);

$coll->remove;

$id = $coll->insert({});
isa_ok($id, 'MongoDB::OID');
$tiny = $coll->find_one;
is($tiny->{'_id'}, $id);

$coll->remove;

# insert
$id = $coll->insert({ just => 'another', perl => 'hacker' });
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

$coll->drop;
ok(!$coll->get_indexes, 'no indexes yet');

my $ok = $coll->ensure_index([qw/foo bar baz/]);
is($ok, 0);
my $err = $db->last_error;
is($err->{ok}, 0);
is($err->{err}, "you're using the old format for ensure_index, ".
   "please check the documentation and update your code");

$ok = $coll->ensure_index([qw/foo bar/]);
is($ok, 0);
$coll->insert({foo => 1, bar => 1, baz => 1, boo => 1});
$coll->insert({foo => 1, bar => 1, baz => 1, boo => 2});
is($coll->count, 2);

$ok = $coll->ensure_index([qw/boo/], "ascending", 1);
is($ok, 0);
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

# make sure this still works
$coll->ensure_index(["foo"]);
@indexes = $coll->get_indexes;
is(scalar @indexes, 2, '1 custom index and the default _id_ index');
$coll->drop;

# test new form of ensure index
{
    $ok = $coll->ensure_index({foo => 1, bar => -1, baz => 1});
    ok($ok);
    $ok = $coll->ensure_index({foo => 1, bar => 1});
    ok($ok);
    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 1});
    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 2});
    is($coll->count, 2);
    
    # unique index
    $coll->ensure_index({boo => 1}, {unique => 1});
    $coll->insert({foo => 3, bar => 3, baz => 3, boo => 2});
    is($coll->count, 2, 'unique index');
}
$coll->drop;


# test doubles
my $pi = 3.14159265;
ok($id = $coll->insert({ data => 'pi', pi => $pi }), "inserting float number value");
ok($obj = $coll->find_one({ data => 'pi' }));
is($obj->{pi}, $pi);

$coll->drop;
my $object = {};
$object->{'autoPartNum'} = '123456';
$object->{'price'} = 123.19;
$coll->insert($object);
my $auto = $coll->find_one;
ok(is_float($auto->{'price'}));
is($auto->{'price'}, $object->{'price'});

# test undefined values
ok($id  = $coll->insert({ data => 'null', none => undef }), 'inserting undefined data');
ok($obj = $coll->find_one({ data => 'null' }), 'finding undefined row');
ok(exists $obj->{none}, 'got null field');
ok(!defined $obj->{none}, 'null field is undefined');

$coll->drop;

# ord("\x9F") is 159
$coll->insert({foo => "\x9F" });
my $utfblah = $coll->find_one;
is(ord($utfblah->{'foo'}), 159, 'translate non-utf8 to utf8 char');

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

# batch insert
$coll->drop;
my $ids = $coll->batch_insert([{'x' => 1}, {'x' => 2}, {'x' => 3}]);
is($coll->count, 3, 'batch_insert');

$cursor = $coll->query->sort({'x' => 1});
my $i = 1;
while ($obj = $cursor->next) {
    is($obj->{'x'}, $i++);
}

# find_one fields
$coll->drop;
$coll->insert({'x' => 1, 'y' => 2, 'z' => 3});
my $yer = $coll->find_one({}, {'y' => 1});

ok(exists $yer->{'y'}, 'y exists');
ok(!exists $yer->{'x'}, 'x doesn\'t');
ok(!exists $yer->{'z'}, 'z doesn\'t');

$coll->drop;
$coll->batch_insert([{"x" => 1}, {"x" => 1}, {"x" => 1}]);
$coll->remove({"x" => 1}, 1);
is ($coll->count, 2, 'remove just one');

# tie::ixhash for update/insert
$coll->drop;
my $hash = Tie::IxHash->new("f" => 1, "s" => 2, "fo" => 4, "t" => 3);
$id = $coll->insert($hash);
isa_ok($id, 'MongoDB::OID');
my $tied = $coll->find_one;
is($tied->{'_id'}."", "$id");
is($tied->{'f'}, 1);
is($tied->{'s'}, 2);
is($tied->{'fo'}, 4);
is($tied->{'t'}, 3);

my $criteria = Tie::IxHash->new("_id" => $id);
$hash->Push("something" => "else");
$coll->update($criteria, $hash);
$tied = $coll->find_one;
is($tied->{'f'}, 1);
is($tied->{'something'}, 'else');


# () update/insert
$coll->drop;
my @h = ("f" => 1, "s" => 2, "fo" => 4, "t" => 3);
$id = $coll->insert(\@h);
isa_ok($id, 'MongoDB::OID');
$tied = $coll->find_one;
is($tied->{'_id'}."", "$id");
is($tied->{'f'}, 1);
is($tied->{'s'}, 2);
is($tied->{'fo'}, 4);
is($tied->{'t'}, 3);

my @criteria = ("_id" => $id);
my @newobj = ('$inc' => {"f" => 1});
$coll->update(\@criteria, \@newobj);
$tied = $coll->find_one;
is($tied->{'f'}, 2);

# update multiple
$coll->drop;
$coll->insert({"x" => 1});
$coll->insert({"x" => 1});

$coll->insert({"x" => 2, "y" => 3});
$coll->insert({"x" => 2, "y" => 4});

$coll->update({"x" => 1}, {'$set' => {'x' => "hi"}});
# make sure one is set, one is not
ok($coll->find_one({"x" => "hi"}));
ok($coll->find_one({"x" => 1}));

# multiple update
$coll->update({"x" => 2}, {'$set' => {'x' => 4}}, {'multiple' => 1});
is($coll->count({"x" => 4}), 2);

$cursor = $coll->query({"x" => 4})->sort({"y" => 1});

$obj = $cursor->next();
is($obj->{'y'}, 3);
$obj = $cursor->next();
is($obj->{'y'}, 4);

# check with upsert if there are matches
SKIP: {
    my $admin = $conn->get_database('admin');
    my $buildinfo = $admin->run_command({buildinfo => 1});
    skip "multiple update won't work with db version $buildinfo->{version}", 5 if $buildinfo->{version} =~ /(0\.\d+\.\d+)|(1\.[12]\d*.\d+)/;

    $coll->update({"x" => 4}, {'$set' => {"x" => 3}}, {'multiple' => 1, 'upsert' => 1}); 
    is($coll->count({"x" => 3}), 2);
    
    $cursor = $coll->query({"x" => 3})->sort({"y" => 1});
    
    $obj = $cursor->next();
    is($obj->{'y'}, 3);
    $obj = $cursor->next();
    is($obj->{'y'}, 4);
    
    # check with upsert if there are no matches
    $coll->update({"x" => 15}, {'$set' => {"z" => 4}}, {'upsert' => 1, 'multiple' => 1});
    ok($coll->find_one({"z" => 4}));
    
    is($coll->count(), 5);
}

$coll->drop;

# test uninitialised array elements
my @g = ();
$g[1] = 'foo';
ok($id = $coll->insert({ data => \@g }));
ok($obj = $coll->find_one());
is_deeply($obj->{data}, [undef, 'foo']);

$coll->drop;

# test PVNV with was float, now string
my $val = 1.5;
$val = 'foo';
ok($id => $coll->insert({ data => $val }));
ok($obj = $coll->find_one({ data => $val }));
is($obj->{data}, 'foo');

# was string, now float
my $f = 'abc';
$f = 3.3;
ok($id => $coll->insert({ data => $f }));
ok($obj = $coll->find_one({ data => $f }));
is($obj->{data}, 3.3);

# safe insert
{
    $coll->drop;
    $coll->insert({_id => 1}, {safe => 1});
    $ok = $coll->insert({_id => 1}, {safe => 1});
    is($ok, 0);
    is($db->last_error->{code}, 11000);
}

# safe remove/update
{
    $coll->drop;
    $ok = $coll->remove;
    is($ok, 1);
    is($db->last_error->{n}, 0);

    $coll->insert({x=>1});
    $ok = $coll->remove({}, {safe => 1});
    is($ok, 1);
    is($db->last_error->{n}, 1);

    $coll->insert({x=>1});
    $ok = $coll->update({}, {'$inc' => {x => 1}});
    is($ok, 1);
    is($db->last_error->{n}, 1);

    $ok = $coll->update({}, {'$inc' => {x => 2}}, {safe => 1});
    is($ok, 1);
    is($db->last_error->{n}, 1);
}

# save
{
    $coll->drop;

    my $x = {"hello" => "world"};
    $coll->save($x);
    is($coll->count, 1);

    my $y = $coll->find_one;
    $y->{"hello"} = 3;
    $coll->save($y);
    is($coll->count, 1);

    my $z = $coll->find_one;
    is($z->{"hello"}, 3);
}

END {
    if ($db) {
        $db->drop;
    }
}
