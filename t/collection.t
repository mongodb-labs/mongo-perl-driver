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
use Test::More 0.96;
use Test::Fatal;
use Test::Warn;

use utf8;
use Data::Types qw(:float);
use Tie::IxHash;
use Encode qw(encode decode);
use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_version server_type/;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);
my $coll;
my $id;
my $obj;
my $ok;
my $cursor;
my $tied;


# get_collection
{
    $testdb->drop;

    $coll = $testdb->get_collection('test_collection');
    isa_ok($coll, 'MongoDB::Collection');

    is($coll->name, 'test_collection', 'get name');

    $testdb->drop;
}

# very small insert
{
    $id = $coll->insert({_id => 1});
    is($id, 1);
    my $tiny = $coll->find_one;
    is($tiny->{'_id'}, 1);

    $coll->remove;

    $id = $coll->insert({});
    isa_ok($id, 'MongoDB::OID');
    $tiny = $coll->find_one;
    is($tiny->{'_id'}, $id);

    $coll->remove;
}

# insert
{
    $id = $coll->insert({ just => 'another', perl => 'hacker' });
    is($coll->count, 1, 'count');

    $coll->update({ _id => $id }, {
        just => "an\xE4oth\0er",
        mongo => 'hacker',
        with => { a => 'reference' },
        and => [qw/an array reference/],
    });
    is($coll->count, 1);
}

# rename 
{
    my $newcoll = $coll->rename('test_collection.rename');
    is($newcoll->name, 'test_collection.rename', 'rename');
    is($coll->count, 0, 'rename');
    is($newcoll->count, 1, 'rename');
    $coll = $newcoll->rename('test_collection');
    is($coll->name, 'test_collection', 'rename');
    is($coll->count, 1, 'rename');
    is($newcoll->count, 0, 'rename');
}

# count
{
    is($coll->count({ mongo => 'programmer' }), 0, 'count = 0');
    is($coll->count({ mongo => 'hacker'     }), 1, 'count = 1');
    is($coll->count({ 'with.a' => 'reference' }), 1, 'inner obj count');
}

# find_one 
{
    $obj = $coll->find_one;
    is($obj->{mongo} => 'hacker', 'find_one');
    is(ref $obj->{with}, 'HASH', 'find_one type');
    is($obj->{with}->{a}, 'reference');
    is(ref $obj->{and}, 'ARRAY');
    is_deeply($obj->{and}, [qw/an array reference/]);
    ok(!exists $obj->{perl});
    is($obj->{just}, "an\xE4oth\0er");
}

# find_one MaxTimeMS
{
    my $err_re = qr/must be non-negative/;
    eval { $coll->find_one({}, {}, { max_time_ms => -1 }) };
    like( $@, $err_re, "find_one sets max_time_ms");
}

# find_one invalid option
{
    my $err_re = qr/max_slime_ms is not/;
    eval { $coll->find_one({}, {}, { max_slime_ms => -1 }) };
    like( $@, $err_re, "max_slime_ms is not a Cursor method");
}

# validate and remove
{
    is( exception { $coll->validate }, undef, 'validate' );

    $coll->remove($obj);
    is($coll->count, 0, 'remove() deleted everything (won\'t work on an old version of Mongo)');
}

# basic indexes
{
    my $res;

    $coll->drop;
    for (my $i=0; $i<10; $i++) {
        $coll->insert({'x' => $i, 'z' => 3, 'w' => 4});
        $coll->insert({'x' => $i, 'y' => 2, 'z' => 3, 'w' => 4});
    }

    $coll->drop;
    ok(!$coll->get_indexes, 'no indexes yet');

    my $indexes = Tie::IxHash->new(foo => 1, bar => 1, baz => 1);
    $res = $coll->ensure_index($indexes);
    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else { 
        ok(!defined $res);
    }

    my $err = $testdb->last_error;
    is($err->{ok}, 1);
    is($err->{err}, undef);

    $indexes = Tie::IxHash->new(foo => 1, bar => 1);
    $res = $coll->ensure_index($indexes);

    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else { 
        ok(!defined $res);
    }

    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 1});
    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 2});
    is($coll->count, 2);

    $res = $coll->ensure_index({boo => 1}, {unique => 1});

    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else { 
        ok(!defined $res);
    }

    eval { $coll->insert({foo => 3, bar => 3, baz => 3, boo => 2}) };

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
    $coll->ensure_index({"foo" => 1});
    @indexes = $coll->get_indexes;
    is(scalar @indexes, 2, '1 custom index and the default _id_ index');
    $coll->drop;
}

# test ensure index with drop_dups
{

    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 1});
    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 2});
    is($coll->count, 2);

    eval { $coll->ensure_index({foo => 1}, {unique => 1}) };
    like( $@, qr/E11000/, "got expected error creating unique index with dups" );

    my $res = $coll->ensure_index({foo => 1}, {unique => 1, drop_dups => 1});

    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else {
        ok(!defined $res);
    }

    $coll->drop;
}


# test new form of ensure index
{
    my $res;
    $res = $coll->ensure_index({foo => 1, bar => -1, baz => 1});

    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else { 
        ok(!defined $res);
    }

    $res = $coll->ensure_index([foo => 1, bar => 1]);

    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else { 
        ok(!defined $res);
    }

    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 1});
    $coll->insert({foo => 1, bar => 1, baz => 1, boo => 2});
    is($coll->count, 2);

    # unique index
    $coll->ensure_index({boo => 1}, {unique => 1});
    eval { $coll->insert({foo => 3, bar => 3, baz => 3, boo => 2}) };
    is($coll->count, 2, 'unique index');

    $coll->drop;
}

# doubles
{
    my $pi = 3.14159265;
    ok($id = $coll->insert({ data => 'pi', pi => $pi }), "inserting float number value");
    ok($obj = $coll->find_one({ data => 'pi' }));
    # can't test exactly because floating point nums are weird
    ok(abs($obj->{pi} - $pi) < .000000001);

    $coll->drop;
    my $object = {};
    $object->{'autoPartNum'} = '123456';
    $object->{'price'} = 123.19;
    $coll->insert($object);
    my $auto = $coll->find_one;
    ok(is_float($auto->{'price'}));
    ok(abs($auto->{'price'} - $object->{'price'}) < .000000001);
}

# undefined values
{
    ok($id  = $coll->insert({ data => 'null', none => undef }), 'inserting undefined data');
    ok($obj = $coll->find_one({ data => 'null' }), 'finding undefined row');
    ok(exists $obj->{none}, 'got null field');
    ok(!defined $obj->{none}, 'null field is undefined');

    $coll->drop;
}

# utf8
{
    my ($down, $up, $non_latin) = ("\xE5", "\xE6", "\x{2603}");
    utf8::upgrade($up);
    utf8::downgrade($down);
    my $insert = { down => $down, up => $up, non_latin => $non_latin };
    my $copy = +{ %{$insert} };
    $coll->insert($insert);
    my $utfblah = $coll->find_one;
    delete $utfblah->{_id};
    is_deeply($utfblah, $copy, 'non-ascii values');

    $coll->drop;

    $insert = { $down => "down", $up => "up", $non_latin => "non_latin" };
    $copy = +{ %{$insert} };
    $coll->insert($insert);
    $utfblah = $coll->find_one;
    delete $utfblah->{_id};
    is_deeply($utfblah, $copy, 'non-ascii keys');
}

# more utf8
{
    local $MongoDB::BSON::utf8_flag_on = 0;
    $coll->drop;
    $coll->insert({"\xe9" => "hi"});
    my $utfblah = $coll->find_one;
    is($utfblah->{"\xC3\xA9"}, "hi", 'byte key');
}

# get_indexes
{
    $coll->drop;
    my $keys = tie(my %idx, 'Tie::IxHash');
    %idx = ('sn' => 1, 'ts' => -1);

    $coll->ensure_index($keys, {safe => 1});

    my @tied = $coll->get_indexes;
    is(scalar @tied, 2, 'num indexes');
    is($tied[1]->{'ns'}, $testdb->name . '.test_collection', 'namespace');
    is($tied[1]->{'name'}, 'sn_1_ts_-1', 'namespace');
}

{
    $coll->drop;

    $coll->insert({x => 1, y => 2, z => 3, w => 4});
    $cursor = $coll->query->fields({'y' => 1});
    $obj = $cursor->next;
    is(exists $obj->{'y'}, 1, 'y exists');
    is(exists $obj->{'_id'}, 1, '_id exists');
    is(exists $obj->{'x'}, '', 'x doesn\'t exist');
    is(exists $obj->{'z'}, '', 'z doesn\'t exist');
    is(exists $obj->{'w'}, '', 'w doesn\'t exist');
}

# batch insert
{
    $coll->drop;
    my $ids = $coll->batch_insert([{'x' => 1}, {'x' => 2}, {'x' => 3}]);
    is($coll->count, 3, 'batch_insert');
}

# sort
{
    $cursor = $coll->query->sort({'x' => 1});
    my $i = 1;
    while ($obj = $cursor->next) {
        is($obj->{'x'}, $i++);
    }
}

# find_one fields
{
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
}

# tie::ixhash for update/insert
{
    $coll->drop;
    my $hash = Tie::IxHash->new("f" => 1, "s" => 2, "fo" => 4, "t" => 3);
    $id = $coll->insert($hash);
    isa_ok($id, 'MongoDB::OID');
    $tied = $coll->find_one;
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
}

# () update/insert
{
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
}

# multiple update
{
    $coll->drop;
    $coll->insert({"x" => 1});
    $coll->insert({"x" => 1});

    $coll->insert({"x" => 2, "y" => 3});
    $coll->insert({"x" => 2, "y" => 4});

    $coll->update({"x" => 1}, {'$set' => {'x' => "hi"}});
    # make sure one is set, one is not
    ok($coll->find_one({"x" => "hi"}));
    ok($coll->find_one({"x" => 1}));

    $coll->update({"x" => 2}, {'$set' => {'x' => 4}}, {'multiple' => 1});
    is($coll->count({"x" => 4}), 2);

    $cursor = $coll->query({"x" => 4})->sort({"y" => 1});

    $obj = $cursor->next();
    is($obj->{'y'}, 3);
    $obj = $cursor->next();
    is($obj->{'y'}, 4);
}

# check with upsert if there are matches
subtest "multiple update" => sub {
    plan skip_all => "multiple update won't work with db version $server_version"
      unless $server_version >= v1.3.0;

    $coll->update({"x" => 4}, {'$set' => {"x" => 3}}, {'multiple' => 1, 'upsert' => 1});
    is($coll->count({"x" => 3}), 2, 'count');

    $cursor = $coll->query({"x" => 3})->sort({"y" => 1});

    $obj = $cursor->next();
    is($obj->{'y'}, 3, 'y == 3');
    $obj = $cursor->next();
    is($obj->{'y'}, 4, 'y == 4');

    # check with upsert if there are no matches
    $coll->update({"x" => 15}, {'$set' => {"z" => 4}}, {'upsert' => 1, 'multiple' => 1});
    ok($coll->find_one({"z" => 4}));

    is($coll->count(), 5);
};


# uninitialised array elements
{
    $coll->drop;
    my @g = ();
    $g[1] = 'foo';
    ok($id = $coll->insert({ data => \@g }));
    ok($obj = $coll->find_one());
    is_deeply($obj->{data}, [undef, 'foo']);
}

# was float, now string
{
    $coll->drop;

    my $val = 1.5;
    $val = 'foo';
    ok($id = $coll->insert({ data => $val }));
    ok($obj = $coll->find_one({ data => $val }));
    is($obj->{data}, 'foo');
}

# was string, now float
{
    my $f = 'abc';
    $f = 3.3;
    ok($id = $coll->insert({ data => $f }), 'insert float');
    ok($obj = $coll->find_one({ data => $f }));
    ok(abs($obj->{data} - 3.3) < .000000001);
}

# timeout
SKIP: {
    skip "buildbot is stupid", 1 if 1;
    my $timeout = $conn->query_timeout;
    $conn->query_timeout(0);

    for (0 .. 10000) {
        $coll->insert({"field1" => "foo", "field2" => "bar", 'x' => $_});
    }

    eval {
        my $num = $testdb->eval('for (i=0;i<1000;i++) { print(.);}');
    };

    ok($@ && $@ =~ /recv timed out/, 'count timeout');

    $conn->query_timeout($timeout);
}

# safe insert
{
    $coll->drop;
    $coll->insert({_id => 1}, {safe => 1});
    eval {$coll->insert({_id => 1}, {safe => 1})};
    ok($@ and $@ =~ /^E11000/, 'duplicate key exception');

  SKIP: {
      skip "the version of the db you're running doesn't give error codes, you may wish to consider upgrading", 1 if !exists $testdb->last_error->{code};

      is($testdb->last_error->{code}, 11000);
    }
}

# safe remove/update
{
    $coll->drop;

    $ok = $coll->remove;
    is($ok, 1, 'unsafe remove');
    is($testdb->last_error->{n}, 0);

    my $syscoll = $testdb->get_collection('system.indexes');
    eval {
        $ok = $syscoll->remove({}, {safe => 1});
    };

    like($@, qr/cannot delete from system namespace|not authorized/, 'remove from system.indexes should fail');

    $coll->insert({x=>1});
    $ok = $coll->update({}, {'$inc' => {x => 1}});
    is($ok->{ok}, 1);

    $ok = $coll->update({}, {'$inc' => {x => 2}}, {safe => 1});
    is($ok->{ok}, 1);
}

# save
{
    $coll->drop;

    my $x = {"hello" => "world"};
    $coll->save($x);
    is($coll->count, 1, 'save');

    my $y = $coll->find_one;
    $y->{"hello"} = 3;
    $coll->save($y);
    is($coll->count, 1);

    my $z = $coll->find_one;
    is($z->{"hello"}, 3);

    my $syscoll = $testdb->get_collection('system.indexes');
    eval {
        $ok = $syscoll->save({_id => 'foo'}, {safe => 1});
    };

    like($@, qr/cannot update system collection|not authorized/, 'save to system.indexes should fail');
}

# find
{
    $coll->drop;

    $coll->insert({x => 1});
    $coll->insert({x => 4});
    $coll->insert({x => 5});
    $coll->insert({x => 1, y => 2});

    $cursor = $coll->find({x=>4});
    my $result = $cursor->next;
    is($result->{'x'}, 4, 'find');

    $cursor = $coll->find({x=>{'$gt' => 1}})->sort({x => -1});
    $result = $cursor->next;
    is($result->{'x'}, 5);
    $result = $cursor->next;
    is($result->{'x'}, 4);

    $cursor = $coll->find({y=>2})->fields({y => 1, _id => 0});
    $result = $cursor->next;
    is(keys %$result, 1, 'find fields');
}


# ns hack
# check insert utf8
{
    my $coll = $testdb->get_collection('test_collection');
    $coll->drop;
    # turn off utf8 flag now
    local $MongoDB::BSON::utf8_flag_on = 0;
    $coll->insert({ foo => "\x{4e2d}\x{56fd}"});
    my $utfblah = $coll->find_one;
    # use utf8;
    my $utfv2 = encode('utf8',"\x{4e2d}\x{56fd}");
    # my $utfv2 = encode('utf8',"中国");
    # diag(Dumper(\$utfv2));
    is($utfblah->{foo},$utfv2,'turn utf8 flag off,return perl internal form(bytes)');
    $coll->drop;
}

# test index names with "."s
{

    $ok = $coll->ensure_index({"x.y" => 1}, {"name" => "foo"});
    my $index = $coll->_database->get_collection("system.indexes")->find_one({"name" => "foo"});
    ok($index);
    ok($index->{'key'});
    ok($index->{'key'}->{'x.y'});
    $coll->drop;
}

# sparse indexes
{
    for (1..10) {
        $coll->insert({x => $_, y => $_}, {safe => 1});
        $coll->insert({x => $_}, {safe => 1});
    }
    is($coll->count, 20);

    eval { $coll->ensure_index({"y" => 1}, {"unique" => 1, "name" => "foo"}) };
    my $index = $coll->_database->get_collection("system.indexes")->find_one({"name" => "foo"});
    ok(!$index);

    $coll->ensure_index({"y" => 1}, {"unique" => 1, "sparse" => 1, "name" => "foo"});
    $index = $coll->_database->get_collection("system.indexes")->find_one({"name" => "foo"});
    ok($index);

    $coll->drop;
}

# text indices
subtest 'text indices' => sub {
    plan skip_all => "text indices won't work with db version $server_version"
        unless $server_version >= v2.4.0;

    my $res = $conn->get_database('admin')->_try_run_command(['getParameter' => 1, 'textSearchEnabled' => 1]);
    plan skip_all => "text search not enabled"
        if !$res->{'textSearchEnabled'};

    my $coll = $testdb->get_collection('test_text');
    $coll->insert({language => 'english', w1 => 'hello', w2 => 'world'}) foreach (1..10);
    is($coll->count, 10);

    $res = $coll->ensure_index({'$**' => 'text'}, {
        name => 'testTextIndex',
        default_language => 'spanish',
        language_override => 'language',
        weights => { w1 => 5, w2 => 10 }
    });

    if ( $server_version >= v2.6.0 ) {
        ok $res->{ok};
    } else { 
        ok(!defined $res);
    }

    my $syscoll = $testdb->get_collection('system.indexes');
    my $text_index = $syscoll->find_one({name => 'testTextIndex'});
    is($text_index->{'default_language'}, 'spanish', 'default_language option works');
    is($text_index->{'language_override'}, 'language', 'language_override option works');
    is($text_index->{'weights'}->{'w1'}, 5, 'weights option works 1');
    is($text_index->{'weights'}->{'w2'}, 10, 'weights option works 2');

    my $search = $testdb->run_command(['text' => 'test_text', 'search' => 'world']);

    # 2.6 changed the response format for text search results, and deprecated
    # the 'text' command. On 2.4, mongos doesn't report the default language
    # and provides stats per shard instead of in total.
    if ( ! ( $server_version >= v2.6.0 || $conn->_is_mongos) ) {
        is($search->{'language'}, 'spanish', 'text search uses preferred language');
        is($search->{'stats'}->{'nfound'}, 10, 'correct number of results found');
    }

    $coll->drop;
};

# utf8 test, croak when null key is inserted
{
    local $MongoDB::BSON::utf8_flag_on = 1;
    $ok = 0;
    my $kanji = "漢\0字";
    utf8::encode($kanji);
    eval{
     $ok = $coll->insert({ $kanji => 1});
    };
    is($ok,0,"Insert key with Null Char Operation Failed");
    is($coll->count, 0, "Insert key with Null Char in Key Failed");
    $coll->drop;
    $ok = 0;
    my $kanji_a = "漢\0字";
    my $kanji_b = "漢\0字中";
    my $kanji_c = "漢\0字国";
    utf8::encode($kanji_a);
    utf8::encode($kanji_b);
    utf8::encode($kanji_c);
    eval {
     $ok = $coll->batch_insert([{ $kanji_a => "some data"} , { $kanji_b => "some more data"}, { $kanji_c => "even more data"}]);
    };
    is($ok,0, "batch_insert key with Null Char in Key Operation Failed");
    is($coll->count, 0, "batch_insert key with Null Char in Key Failed");
    $coll->drop;

    #test ixhash
    my $hash = Tie::IxHash->new("f\0f" => 1);
    eval {
     $ok = $coll->insert($hash);
    };
    is($ok,0, "ixHash Insert key with Null Char in Key Operation Failed");
    is($coll->count, 0, "ixHash key with Null Char in Key Operation Failed");
    $tied = $coll->find_one;
    $coll->drop;
}

# findAndModify
{
    $coll->insert( { name => "find_and_modify_test", value => 42 } );
    $coll->find_and_modify( { query => { name => "find_and_modify_test" }, update => { '$set' => { value => 43 } } } );
    my $doc = $coll->find_one( { name => "find_and_modify_test" } );
    is( $doc->{value}, 43 );

    $coll->drop;

    $coll->insert( { name => "find_and_modify_test", value => 46 } );
    my $new = $coll->find_and_modify( { query  => { name => "find_and_modify_test" }, 
                                        update => { '$set' => { value => 57 } },
                                        new    => 1 } );

    is ( $new->{value}, 57 );

    $coll->drop;

    my $nothing = $coll->find_and_modify( { query => { name => "does not exist" }, update => { name => "barf" } } );

    is ( $nothing, undef );

    $coll->drop;
}

# aggregate 
subtest "aggregation" => sub {
    plan skip_all => "Aggregation framework unsupported on MongoDB $server_version"
        unless $server_version >= v2.2.0;

    $coll->batch_insert( [ { wanted => 1, score => 56 },
                           { wanted => 1, score => 72 },
                           { wanted => 1, score => 96 },
                           { wanted => 1, score => 32 },
                           { wanted => 1, score => 61 },
                           { wanted => 1, score => 33 },
                           { wanted => 0, score => 1000 } ] );

    my $res = $coll->aggregate( [ { '$match'   => { wanted => 1 } },
                                  { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } } ] );

    is( ref( $res ), ref [ ] );
    ok $res->[0]{avgScore} < 59;
    ok $res->[0]{avgScore} > 57;

    if ( $server_version < v2.5.0 ) {
        like(
            exception { $coll->aggregate( [ {'$match' => { count => {'$gt' => 0} } } ], { cursor => 1 } ) },
            qr/unrecognized field.*cursor/,
            "asking for cursor when unsupported throws error"
        );
    }
};

# aggregation cursors
subtest "aggregation cursors" => sub {
    plan skip_all => "Aggregation cursors unsupported on MongoDB $server_version"
        unless $server_version >= v2.5.0;

    for( 1..20 ) { 
        $coll->insert( { count => $_ } );
    }

    $cursor = $coll->aggregate( [ { '$match' => { count => { '$gt' => 0 } } } ], { cursor => 1 } );

    isa_ok $cursor, 'MongoDB::Cursor';
    is $cursor->started_iterating, 1;
    is( ref( $cursor->_docs ), ref [ ] );
    is $cursor->_doc_count, 20, "document count cached in cursor";

    for( 1..20 ) { 
        my $doc = $cursor->next;
        is( ref( $doc ), ref { } );
        is $doc->{count}, $_;
        is $cursor->_doc_count, ( 20 - $_ );
    }

    # make sure we can transition to a "real" cursor
    $cursor = $coll->aggregate( [ { '$match' => { count => { '$gt' => 0 } } } ], { cursor => { batchSize => 10 } } );

    isa_ok $cursor, 'MongoDB::Cursor';
    is $cursor->started_iterating, 1;
    is( ref( $cursor->_docs), ref [ ] );
    is $cursor->_doc_count, 10;

    for( 1..20 ) { 
        my $doc = $cursor->next;
        is( ref( $doc ), ref { } );
        is $doc->{count}, $_;
    }

    $coll->drop;
};

# aggregation $out
subtest "aggregation \$out" => sub {
    plan skip_all => "Aggregation result collections unsupported on MongoDB $server_version"
        unless $server_version >= v2.5.0;

    for( 1..20 ) {
        $coll->insert( { count => $_ } );
    }

    my $result = $coll->aggregate( [ { '$match' => { count => { '$gt' => 0 } } }, { '$out' => 'test_out' } ] );

    ok $result;
    my $res_coll = $testdb->get_collection( 'test_out' );
    my $cursor = $res_coll->find;

    for( 1..20 ) {
        my $doc = $cursor->next;
        is( ref( $doc ), ref { } );
        is $doc->{count}, $_;
    }

    $res_coll->drop;
    $coll->drop;
};

# aggregation explain
subtest "aggregation explain" => sub {
    plan skip_all => "Aggregation explain unsupported on MongoDB $server_version"
        unless $server_version >= v2.4.0;

    for ( 1..20 ) {
        $coll->insert( { count => $_ } );
    }

    my $result = $coll->aggregate( [ { '$match' => { count => { '$gt' => 0 } } }, { '$sort' => { count => 1 } } ], 
                                   { explain => 1 } );

    is( ref( $result ), 'HASH', "aggregate with explain returns a hashref" );

    my $expected = $server_version >= v2.6.0 ? 'stages' : 'serverPipeline';

    ok( exists $result->{$expected}, "result had '$expected' field" )
        or diag explain $result;

    $coll->drop;
};

# parallel_scan
subtest "parallel scan" => sub {
    plan skip_all => "Parallel scan not supported before MongoDB 2.6"
        unless $server_version >= v2.6.0;
    plan skip_all => "Parallel scan not supported on mongos"
        if $server_type eq 'Mongos';

    my $num_docs = 2000;

    for ( 1..$num_docs ) {
        $coll->insert( { _id => $_ } );
    }

    my $err_re = qr/must be a positive integer between 1 and 10000/;

    eval { $coll->parallel_scan };
    like( $@, $err_re, "parallel_scan() throws error");

    for my $i ( 0, -1, 10001 ) {
        eval { $coll->parallel_scan($i) };
        like( $@, $err_re, "parallel_scan($i) throws error" );
    }

    my $max = 3;
    my @cursors = $coll->parallel_scan($max);
    ok( scalar @cursors <= $max, "parallel_scan($max) returned <= $max cursors" );

    for my $method ( qw/reset count explain/ ) {
        eval { $cursors[0]->$method };
        like( $@, qr/cannot $method a parallel scan/, "$method on parallel scan cursor throws error" );
    }

    _check_parallel_results( $num_docs, @cursors );

    # read preference
    subtest "replica set" => sub {
        plan skip_all => 'needs a replicaset'
            unless $server_type eq 'RSPrimary';

        my $conn2 = MongoDBTest::build_client();
        $conn2->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);

        my @cursors = $coll->parallel_scan($max);
        _check_parallel_results( $num_docs, @cursors );
    };

    # empty collection
    subtest "empty collection" => sub {
        $coll->remove({});
        my @cursors = $coll->parallel_scan($max);
        _check_parallel_results( 0, @cursors );
    }

};

sub _check_parallel_results {
    my ($num_docs, @cursors) = @_;
    local $Test::Builder::Level = $Test::Builder::Level+1;

    my %seen;
    my $count;
    for my $i (0 .. $#cursors ) {
        my @chunk = $cursors[$i]->all;
        if ( $num_docs ) {
            ok( @chunk > 0, "cursor $i had some results" );
        }
        else {
            is( scalar @chunk, 0, "cursor $i had no results" );
        }
        $seen{$_}++ for map { $_->{_id} } @chunk;
        $count += @chunk;
    }
    is( $count, $num_docs, "cursors returned right number of docs" );
    is_deeply( [sort { $a <=> $b } keys %seen], [ 1 .. $num_docs], "cursors returned all results" );

}

subtest "deep update" => sub {
    $coll->drop;
    $coll->insert( { _id => 1 } );

    $coll->update( { _id => 1 }, { '$set' => { 'x.y' => 42 } } );

    my $doc = $coll->find_one( { _id => 1 } );
    is( $doc->{x}{y}, 42, "deep update worked" );

    like(
        exception { $coll->update( { _id => 1 }, { 'p.q' => 23 } ) },
        qr/documents for storage cannot contain/,
        "replace with dots in field dies"
    );

};

done_testing;
