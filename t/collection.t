#  Copyright 2009 - present MongoDB, Inc.
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

use strict;
use warnings;
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;
use Encode qw(encode decode);
use MongoDB::Error;

use MongoDB;
use BSON::Types ':all';

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    check_min_server_version
    skip_unless_min_version
/;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);
my $coll = $testdb->get_collection('test_collection');

my $supports_collation = $server_version >= 3.3.9;
my $case_insensitive_collation = { locale => "en_US", strength => 2 };

my $id;
my $obj;
my $ok;
my $cursor;
my $tied;


# get_collection
subtest get_collection => sub {
    my ( $db, $c );

    ok( $c = $testdb->get_collection('foo'), "get_collection(NAME)" );
    isa_ok( $c, 'MongoDB::Collection' );
    is( $c->name, 'foo', 'get name' );

    my $wc = MongoDB::WriteConcern->new( w => 2 );

    ok( $c = $testdb->get_collection( 'foo', { write_concern => $wc } ),
        "get_collection(NAME, OPTION) (wc)" );
    is( $c->write_concern->w, 2, "coll-level write concern as expected" );

    ok( $c = $testdb->get_collection( 'foo', { write_concern => { w => 3 } } ),
        "get_collection(NAME, OPTION) (wc)" );
    is( $c->write_concern->w, 3, "coll-level write concern coerces" );

    my $rp = MongoDB::ReadPreference->new( mode => 'secondary' );

    ok( $c = $testdb->get_collection( 'foo', { read_preference => $rp } ),
        "get_collection(NAME, OPTION) (rp)" );
    is( $c->read_preference->mode, 'secondary', "coll-level read pref as expected" );

    ok( $c = $testdb->get_collection( 'foo', { read_preference => { mode => 'nearest' } } ),
        "get_collection(NAME, OPTION) (rp)" );
    is( $c->read_preference->mode, 'nearest', "coll-level read pref coerces" );

};

subtest get_namespace => sub {
    my $dbname = $testdb->name;
    my ( $db, $c );

    ok( $c = $conn->get_namespace("$dbname.foo"), "get_namespace(NAME)" );
    isa_ok( $c, 'MongoDB::Collection' );
    is( $c->name, 'foo', 'get name' );

    my $wc = MongoDB::WriteConcern->new( w => 2 );

    ok( $c = $conn->get_namespace( "$dbname.foo", { write_concern => $wc } ),
        "get_collection(NAME, OPTION) (wc)" );
    is( $c->write_concern->w, 2, "coll-level write concern as expected" );

    ok( $c = $conn->ns("$dbname.foo"), "ns(NAME)" );
    isa_ok( $c, 'MongoDB::Collection' );
    is( $c->name, 'foo', 'get name' );
};

# very small insert
{
    $id = $coll->insert_one({_id => 1})->inserted_id;
    is($id, 1);
    my $tiny = $coll->find_one;
    is($tiny->{'_id'}, 1);

    $coll->drop;

    $id = $coll->insert_one({})->inserted_id;
    isa_ok($id, 'BSON::OID');
    $tiny = $coll->find_one;
    is($tiny->{'_id'}, $id);

    $coll->drop;
}

subtest write_concern => sub {
    my $c;

    ok( $c = $testdb->get_collection( 'foo', { write_concern => { w => 999 } } ),
        "get collection with w=999" );
    my $err = exception { $c->insert_one( { _id => 1 } ) };
    ok(ref $err && $err->isa('MongoDB::DatabaseError'),
        "collection-level write concern applies to insert_one"
    ) or diag "got:", explain $err;
};

# inserting an _id subdoc with $ keys should be an error; only on 2.4+
unless ( check_min_server_version($conn, 'v2.4.11') ) {
    like(
        exception {
            $coll->insert_one( { '_id' => { '$oid' => "52d0b971b3ba219fdeb4170e" } } )
        },
        qr/WriteError/,
        "inserting an _id subdoc with \$ keys should error"
    );
}

# insert
{
    $coll->drop;

    $id = $coll->insert_one({ just => 'another', perl => 'hacker' })->inserted_id;
    is($coll->count_documents({}), 1, 'count');

    $coll->replace_one({ _id => $id }, {
        just => "an\xE4oth\0er",
        mongo => 'hacker',
        with => { a => 'reference' },
        and => [qw/an array reference/],
    });
    is($coll->count_documents({}), 1);
}

# rename
{
    my $newcoll = $coll->rename('test_collection.rename');
    is($newcoll->name, 'test_collection.rename', 'rename');
    is($coll->count_documents({}), 0, 'rename');
    is($newcoll->count_documents({}), 1, 'rename');
    $coll = $newcoll->rename('test_collection');
    is($coll->name, 'test_collection', 'rename');
    is($coll->count_documents({}), 1, 'rename');
    is($newcoll->count_documents({}), 0, 'rename');
}

# count_documents
{
    is($coll->count_documents({ mongo => 'programmer' }), 0, 'count = 0');
    is($coll->count_documents({ mongo => 'hacker'     }), 1, 'count = 1');
    is($coll->count_documents({ 'with.a' => 'reference' }), 1, 'inner obj count');

    # requires filter
    like( exception { $coll->count_documents },
        qr/MongoDB::UsageError/, "requires a filter" );

    # missing collection
    my $coll2 = $testdb->coll("aadfkasfa");
    my $count;
    is(
        exception { $count = $coll2->count_documents({}) },
        undef,
        "count on missing collection lives"
    );
    is( $count, 0, "count is correct" );
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

# find_id
{
  my $doc = { a => 1, b => 2, c => 3 };
  my $id = $coll->insert_one($doc)->inserted_id;
  my $result = $coll->find_id($id);
  is($result->{_id}, $id, 'find_id');

  $result = $coll->find_id($id, { c => 3 });
  cmp_deeply(
    $result,
    { _id => str($id), c => num(3) }, # use str() to allow MongoDB::OID vs BSON::OID
    "find_id projection"
  );

  $coll->delete_one($result);
}

# remove
{
    $coll->delete_one($obj);
    is($coll->count_documents({}), 0, 'remove() deleted everything (won\'t work on an old version of Mongo)');
}

# doubles
{
    my $pi = 3.14159265;
    ok($id = $coll->insert_one({ data => 'pi', pi => $pi })->inserted_id, "inserting float number value");
    ok($obj = $coll->find_one({ data => 'pi' }));
    # can't test exactly because floating point nums are weird
    ok(abs($obj->{pi} - $pi) < .000000001);

    $coll->drop;
    my $object = {};
    $object->{'autoPartNum'} = '123456';
    $object->{'price'} = 123.19;
    $coll->insert_one($object);
    my $auto = $coll->find_one;
    like($auto->{'price'}, qr/^123\.\d+/, "round trip float looks like float");
    ok(abs($auto->{'price'} - $object->{'price'}) < .000000001);
}

# undefined values
{
    ok($id  = $coll->insert_one({ data => 'null', none => undef })->inserted_id, 'inserting undefined data');
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
    $coll->insert_one($insert);
    my $utfblah = $coll->find_one;
    delete $utfblah->{_id};
    is_deeply($utfblah, $copy, 'non-ascii values');

    $coll->drop;

    $insert = { $down => "down", $up => "up", $non_latin => "non_latin" };
    $copy = +{ %{$insert} };
    $coll->insert_one($insert);
    $utfblah = $coll->find_one;
    delete $utfblah->{_id};
    is_deeply($utfblah, $copy, 'non-ascii keys');
}

# more utf8
{
    $coll->drop;
    $coll->insert_one({"\xe9" => "hi"});
    my $utfblah = $coll->find_one;
    is($utfblah->{"\xe9"}, "hi", 'byte key');
}

{
    $coll->drop;

    $coll->insert_one({x => 1, y => 2, z => 3, w => 4});
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
    my $ids = $coll->insert_many([{'x' => 1}, {'x' => 2}, {'x' => 3}])->inserted_ids;
    is($coll->count_documents({}), 3, 'insert_many');
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
    $coll->insert_one({'x' => 1, 'y' => 2, 'z' => 3})->inserted_id;
    my $yer = $coll->find_one({}, {'y' => 1});

    cmp_deeply(
        $yer,
        { _id => ignore(), y => num(2) },
        "projection fields correct"
    );

    $coll->drop;
    $coll->insert_many([{"x" => 1}, {"x" => 1}, {"x" => 1}]);
    $coll->delete_one( { "x" => 1 } );
    is ($coll->count_documents({}), 2, 'remove just one');
}

# tie::ixhash for update/insert
{
    $coll->drop;
    my $hash = Tie::IxHash->new("f" => 1, "s" => 2, "fo" => 4, "t" => 3);
    $id = $coll->insert_one($hash)->inserted_id;
    isa_ok($id, 'BSON::OID');
    $tied = $coll->find_one;
    is($tied->{'_id'}."", "$id");
    is($tied->{'f'}, 1);
    is($tied->{'s'}, 2);
    is($tied->{'fo'}, 4);
    is($tied->{'t'}, 3);

    my $criteria = Tie::IxHash->new("_id" => $id);
    $hash->Push("something" => "else");
    $coll->replace_one($criteria, $hash);
    $tied = $coll->find_one;
    is($tied->{'f'}, 1);
    is($tied->{'something'}, 'else');
}

subtest "BSON::Doc for insert/update" => sub {
    $coll->drop;
    my $hash = bson_doc("f" => 1, "s" => 2, "fo" => 4, "t" => 3);
    $id = $coll->insert_one($hash)->inserted_id;
    isa_ok($id, 'BSON::OID');
    my $got = $coll->find_one;
    is($got->{'_id'}."", "$id");
    is($got->{'f'}, 1);
    is($got->{'s'}, 2);
    is($got->{'fo'}, 4);
    is($got->{'t'}, 3);

    my $criteria = bson_doc("_id" => $id);
    $coll->replace_one($criteria, bson_doc( f => 1, something => 'else' ));
    $got = $coll->find_one;
    is($got->{'f'}, 1);
    is($got->{'something'}, 'else');

    my $oid = bson_oid();
    ok( $coll->insert_one(bson_doc(_id => $oid)), "inserted with oid" );
    ok( $coll->find_id($oid), "found by oid" );
};

# () update/insert
{
    $coll->drop;
    my @h = ("f" => bson_int32(1), "s" => 2, "fo" => 4, "t" => 3);
    $id = $coll->insert_one(\@h)->inserted_id;
    isa_ok($id, 'BSON::OID');
    $tied = $coll->find_one;
    is($tied->{'_id'}."", "$id");
    is($tied->{'f'}, 1);
    is($tied->{'s'}, 2);
    is($tied->{'fo'}, 4);
    is($tied->{'t'}, 3);

    my @criteria = ("_id" => $id);
    my @newobj = ('$inc' => {"f" => 1});
    $coll->update_one(\@criteria, \@newobj);
    $tied = $coll->find_one;
    is($tied->{'f'}, 2);
}

# multiple update
{
    $coll->drop;
    $coll->insert_one({"x" => 1});
    $coll->insert_one({"x" => 1});

    $coll->insert_one({"x" => 2, "y" => 3});
    $coll->insert_one({"x" => 2, "y" => 4});

    $coll->update_one({"x" => 1}, {'$set' => {'x' => "hi"}});
    # make sure one is set, one is not
    ok($coll->find_one({"x" => "hi"}));
    ok($coll->find_one({"x" => 1}));

    my $res = $coll->update_many({"x" => 2}, {'$set' => {'x' => 4}});
    is($coll->count_documents({"x" => 4}), 2) or diag explain $res;

    $cursor = $coll->query({"x" => 4})->sort({"y" => 1});

    $obj = $cursor->next();
    is($obj->{'y'}, 3);
    $obj = $cursor->next();
    is($obj->{'y'}, 4);
}

# check with upsert if there are matches
subtest "multiple update" => sub {
    skip_unless_min_version($conn, 'v1.3.0');

    $coll->update_many({"x" => 4}, {'$set' => {"x" => 3}}, {'upsert' => 1});
    is($coll->count_documents({"x" => 3}), 2, 'count');

    $cursor = $coll->query({"x" => 3})->sort({"y" => 1});

    $obj = $cursor->next();
    is($obj->{'y'}, 3, 'y == 3');
    $obj = $cursor->next();
    is($obj->{'y'}, 4, 'y == 4');
};


# uninitialised array elements
{
    $coll->drop;
    my @g = ();
    $g[1] = 'foo';
    ok($id = $coll->insert_one({ data => \@g })->inserted_id);
    ok($obj = $coll->find_one());
    is_deeply($obj->{data}, [undef, 'foo']);
}

# was float, now string
{
    $coll->drop;

    my $val = 1.5;
    $val = 'foo';
    ok($id = $coll->insert_one({ data => $val })->inserted_id);
    ok($obj = $coll->find_one({ data => $val }));
    is($obj->{data}, 'foo');
}

# was string, now float
{
    my $f = 'abc';
    $f = 3.3;
    ok($id = $coll->insert_one({ data => $f })->inserted_id, 'insert float');
    ok($obj = $coll->find_one({ data => $f }));
    ok(abs($obj->{data} - 3.3) < .000000001);
}

# timeout
SKIP: {
    skip "buildbot is stupid", 1 if 1;
    my $timeout = $conn->query_timeout;
    $conn->query_timeout(0);

    for (0 .. 10000) {
        $coll->insert_one({"field1" => "foo", "field2" => "bar", 'x' => $_});
    }

    eval {
        # XXX eval is deprecated, but we'll leave this test for now
        my $num = $testdb->eval('for (i=0;i<1000;i++) { print(.);}');
    };

    ok($@ && $@ =~ /recv timed out/, 'count timeout');

    $conn->query_timeout($timeout);
}

# safe insert
{
    $coll->drop;
    $coll->insert_one({_id => 1});
    my $err = exception { $coll->insert_one({_id => 1}) };
    ok( $err, "got error" );
    isa_ok( $err, 'MongoDB::DatabaseError', "duplicate insert error" );
    like( $err->message, qr/duplicate key/, 'error was duplicate key exception')
}

# find
{
    $coll->drop;

    $coll->insert_one({x => 1});
    $coll->insert_one({x => 4});
    $coll->insert_one({x => 5});
    $coll->insert_one({x => 1, y => 2});

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

# batch
{
    $coll->drop;

    for (0..14) { $coll->insert_one({ x => $_ }) };

    $cursor = $coll->find({} , { batchSize => 5 });

    my @batch = $cursor->batch;
    is(scalar @batch, 5, 'batch');

    $cursor->next;
    $cursor->next;
    @batch = $cursor->batch;
    is(scalar @batch, 3, 'batch with next');

    @batch = $cursor->batch;
    is(scalar @batch, 5, 'batch after next');

    @batch = $cursor->batch;
    ok(!@batch, 'empty batch');
}


# ns hack
# check insert utf8
{
    my $coll = $testdb->get_collection('test_collection');
    $coll->drop;
    my $utf8 = "\x{4e2d}\x{56fd}";
    $coll->insert_one({ foo => $utf8});
    my $utfblah = $coll->find_one;
    is($utfblah->{foo}, $utf8,'round trip UTF-8');
    $coll->drop;
}

# utf8 test, croak when null key is inserted
{
    $ok = 0;
    my $kanji = "漢\0字";
    utf8::encode($kanji);
    eval{
     $ok = $coll->insert_one({ $kanji => 1});
    };
    is($ok,0,"Insert key with Null Char Operation Failed");
    is($coll->count_documents({}), 0, "Insert key with Null Char in Key Failed");
    $coll->drop;
    $ok = 0;
    my $kanji_a = "漢\0字";
    my $kanji_b = "漢\0字中";
    my $kanji_c = "漢\0字国";
    utf8::encode($kanji_a);
    utf8::encode($kanji_b);
    utf8::encode($kanji_c);
    eval {
     $ok = $coll->insert_many([{ $kanji_a => "some data"} , { $kanji_b => "some more data"}, { $kanji_c => "even more data"}]);
    };
    is($ok,0, "insert_many key with Null Char in Key Operation Failed");
    is($coll->count_documents({}), 0, "insert_many key with Null Char in Key Failed");
    $coll->drop;

    #test ixhash
    my $hash = Tie::IxHash->new("f\0f" => 1);
    eval {
     $ok = $coll->insert_one($hash);
    };
    is($ok,0, "ixHash Insert key with Null Char in Key Operation Failed");
    is($coll->count_documents({}), 0, "ixHash key with Null Char in Key Operation Failed");
    $tied = $coll->find_one;
    $coll->drop;
}

# aggregate
subtest "aggregation" => sub {
    skip_unless_min_version($conn, 'v2.2.0');

    $coll->insert_many( [ { wanted => 1, score => 56 },
                           { wanted => 1, score => 72 },
                           { wanted => 1, score => 96 },
                           { wanted => 1, score => 32 },
                           { wanted => 1, score => 61 },
                           { wanted => 1, score => 33 },
                           { wanted => 0, score => 1000 } ] );

    my $cursor = $coll->aggregate( [ { '$match'   => { wanted => 1 } },
                                  { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } } ] );

    isa_ok( $cursor, 'MongoDB::QueryResult' );
    my $res = [ $cursor->all ];
    ok $res->[0]{avgScore} < 59;
    ok $res->[0]{avgScore} > 57;

    if ( check_min_server_version($conn, 'v2.5.0') ) {
        is(
            exception { $coll->aggregate( [ {'$match' => { count => {'$gt' => 0} } } ], { cursor => {} } ) },
            undef,
            "asking for cursor when unsupported does not throw error"
        );
    }
};

# aggregation cursors
subtest "aggregation cursors" => sub {
    skip_unless_min_version($conn, 'v2.5.0');

    for( 1..20 ) {
        $coll->insert_one( { count => $_ } );
    }

    $cursor = $coll->aggregate( [ { '$match' => { count => { '$gt' => 0 } } } ], { cursor => 1 } );

    isa_ok $cursor, 'MongoDB::QueryResult';
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

    isa_ok $cursor, 'MongoDB::QueryResult';
    is $cursor->started_iterating, 1;
    is( ref( $cursor->_docs), ref [ ] );
    is $cursor->_doc_count, 10, "doc count correct";

    for( 1..20 ) {
        my $doc = $cursor->next;
        isa_ok( $doc, 'HASH' );
        is $doc->{count}, $_, "doc count field is $_";
    }

    $coll->drop;
};

# aggregation $out
subtest "aggregation \$out" => sub {
    skip_unless_min_version($conn, 'v2.5.0');

    for( 1..20 ) {
        $coll->insert_one( { count => $_ } );
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
    skip_unless_min_version($conn, 'v2.4.0');

    for ( 1..20 ) {
        $coll->insert_one( { count => $_ } );
    }

    my $cursor = $coll->aggregate( [ { '$match' => { count => { '$gt' => 0 } } }, { '$sort' => { count => 1 } } ],
                                   { explain => 1 } );

    my $result = $cursor->next;

    is( ref( $result ), 'HASH', "aggregate with explain returns a hashref" );

    my $expected = $server_version >= v2.6.0 ? 'stages' : 'serverPipeline';

    ok( exists $result->{$expected}, "result had '$expected' field" )
        or diag explain $result;

    $coll->drop;
};

# aggregation index hints
subtest "aggregation index hint string" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->insert_many( [ { _id => 1, category => "cake", type => "chocolate", qty => 10 },
                          { _id => 2, category => "cake", type => "ice cream", qty => 25 },
                          { _id => 3, category => "pie", type => "boston cream", qty => 20 },
                          { _id => 4, category => "pie", type => "blueberry", qty => 15 } ] );

    # creating two indicies to give the planner a choice
    $coll->indexes->create_one( [ qty => 1, type => 1 ] );
    my $index_name = $coll->indexes->create_one( [ qty => 1, category => 1 ] );

    my $cursor_no_hint = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } }
        ],
        { explain => 1 }
    );

    my $cursor_with_hint = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } } ],
        { hint => $index_name, explain => 1 }
    );

    my $result_no_hint = $cursor_no_hint->next;

    is( ref( $result_no_hint ), 'HASH', "aggregate with explain returns a hashref" );

    ok(
        scalar( @{ $result_no_hint->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) > 0,
        "aggregate with no hint had rejectedPlans",
    );

    my $result_with_hint = $cursor_with_hint->next;

    is( ref( $result_with_hint ), 'HASH', "aggregate with explain returns a hashref" );

    ok(
        scalar( @{ $result_with_hint->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) == 0,
        "aggregate with hint had no rejectedPlans",
    );

    $coll->drop;
};

subtest "aggregation index hint object" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->insert_many( [ { _id => 1, category => "cake", type => "chocolate", qty => 10 },
                          { _id => 2, category => "cake", type => "ice cream", qty => 25 },
                          { _id => 3, category => "pie", type => "boston cream", qty => 20 },
                          { _id => 4, category => "pie", type => "blueberry", qty => 15 } ] );

    # creating two indicies to give the planner a choice
    $coll->indexes->create_one( [ qty => 1, type => 1 ] );
    $coll->indexes->create_one( [ qty => 1, category => 1 ] );

    my $cursor_no_hint = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } }
        ],
        { explain => 1 }
    );

    my $cursor_with_hint = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } } ],
        { hint => Tie::IxHash->new( qty => 1, category => 1 ), explain => 1 }
    );

    my $result_no_hint = $cursor_no_hint->next;

    is( ref( $result_no_hint ), 'HASH', "aggregate with explain returns a hashref" );

    ok(
        scalar( @{ $result_no_hint->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) > 0,
        "aggregate with no hint had rejectedPlans",
    );

    my $result_with_hint = $cursor_with_hint->next;

    is( ref( $result_with_hint ), 'HASH', "aggregate with explain returns a hashref" );

    ok(
        scalar( @{ $result_with_hint->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) == 0,
        "aggregate with hint had no rejectedPlans",
    );

    $coll->drop;
};

subtest "aggregation with collation" => sub {
    $coll->insert_one( { _id => "foo" } );

    if ($supports_collation) {
        my @result = $coll->aggregate(
            [ { '$match' => { _id => "FOO" } } ],
            { collation => $case_insensitive_collation },
        )->all;
        is_deeply( \@result, [ { _id => "foo" } ], "aggregate with collation" );
    }
    else {
        like(
            exception {
                $coll->aggregate(
                    [ { '$match' => { _id => "FOO" } } ],
                    { collation => $case_insensitive_collation },
                )->all;
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "aggregate w/ collation returns error if unsupported"
        );
    }
};

subtest "deep update" => sub {
    $coll->drop;
    $coll->insert_one( { _id => 1 } );

    $coll->update_one( { _id => 1 }, { '$set' => { 'x.y' => 42 } } );

    my $doc = $coll->find_one( { _id => 1 } );
    is( $doc->{x}{y}, 42, "deep update worked" );

    like(
        exception { $coll->replace_one( { _id => 1 }, { 'p.q' => 23 } ) },
        qr/documents for storage cannot contain|has invalid character/,
        "replace with dots in field dies"
    );

};

subtest "count_document w/ hint" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->drop;
    $coll->insert_one( { i => 1 } );
    $coll->insert_one( { i => 2 } );
    is ($coll->count_documents({}), 2, 'count = 2');

    $coll->indexes->create_one( { i => 1 } );

    is( $coll->count_documents( { i => 1 }, { hint => '_id_' } ), 1, 'count w/ hint & spec');
    is( $coll->count_documents( {}, { hint => '_id_' } ), 2, 'count w/ hint');

    my $current_version = version->parse($server_version);
    my $version_2_6 = version->parse('v2.6');
    if ( $current_version > $version_2_6 ) {

        eval { $coll->count_documents( { i => 1 } , { hint => 'BAD HINT' } ) };
        like($@, qr/failed|bad hint|hint provided does not correspond/, 'check bad hint error');

    } else {

        is( $coll->count_documents( { i => 1 } , { hint => 'BAD HINT' } ), 1, 'bad hint and spec');
    }

    $coll->indexes->create_one( { x => 1 }, { sparse => 1 } );

    if ( $current_version > $version_2_6 ) {

        is( $coll->count_documents( {  i => 1 } , { hint => 'x_1' } ), 0, 'spec & hint on empty sparse index');

    } else {

        is( $coll->count_documents( {  i => 1 } , { hint => 'x_1' } ), 1, 'spec & hint on empty sparse index');
    }

    # XXX Failing on nightly master -- xdg, 2016-02-11
    TODO: {
        local $TODO = "Failing nightly master";
        is( $coll->count_documents( {}, { hint => 'x_1' } ), 2, 'hint on empty sparse index');
    }
};

subtest "count w/ collation" => sub {
    $coll->drop;
    $coll->insert_one( { x => "foo" } );

    if ($supports_collation) {
        is( $coll->count_documents( { x => "FOO" }, { collation => $case_insensitive_collation } ),
            1, 'count w/ collation' );
    }
    else {
        like(
            exception {
                $coll->count_documents( { x => "FOO" }, { collation => $case_insensitive_collation } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "count w/ collation returns error if unsupported"
        );
    }
};

subtest "distinct w/ collation" => sub {
    $coll->drop;
    $coll->insert_one( { x => "foo" } );
    $coll->insert_one( { x => "FOO" } );

    if ($supports_collation) {
        my $num_distinct =
          $coll->distinct( "x", {}, { collation => $case_insensitive_collation } )->all;
        is( $num_distinct, 1, "distinct w/ collation" );
    }
    else {
        like(
            exception {
                $coll->distinct( "x", {}, { collation => $case_insensitive_collation } )->all;
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "distinct w/ collation returns error if unsupported"
        );
    }
};

subtest "querying w/ collation" => sub {
    $coll->drop;
    $coll->insert_one( { _id => 0, x => "FOO" } );
    $coll->insert_one( { _id => 1, x => "foo" } );

    if ($supports_collation) {
        my $result_count =
          $coll->find( { x => "foo" }, { collation => $case_insensitive_collation } )->all;
        is( $result_count, 2, "find w/ collation" );

        my $doc = $coll->find_one( { _id => 0, x => "foo" },
            undef, { collation => $case_insensitive_collation } );
        cmp_deeply( $doc, { _id => num(0), x => str("FOO") }, "find_one w/ collation" );
    }
    else {
        like(
            exception {
                $coll->find( { x => "foo" }, { collation => $case_insensitive_collation } )->all;
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "find w/ collation returns error if unsupported"
        );

        like(
            exception {
                $coll->find_one( { _id => 0, x => "foo" },
                    undef, { collation => $case_insensitive_collation } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "find_one w/ collation returns error if unsupported"
        );
    }
};

subtest "bulk_write writeConcern used" => sub {
    plan skip_all => "Test requires ReplicaSet"
        unless $server_type eq 'RSPrimary';

    $coll->drop;

    like exception {
        $coll->bulk_write(
            [insert_one => [{ x => 3 }]],
            { writeConcern => { w => 999 } },
        );
    }, qr/WriteConcernError/, 'write concern is taken into account';
};

my $js_str = 'function() { return this.a > this.b }';
my $js_obj = bson_code ($js_str );

for my $criteria ( $js_str, $js_obj ) {
    my $type = ref($criteria) || 'string';
    subtest "query with \$where as $type" => sub {
        plan skip_all => "Not supported on Atlas Free Tier"
          if $ENV{ATLAS_PROXY};

        $coll->drop;
        $coll->insert_one( { a => 1, b => 1, n => 1 } );
        $coll->insert_one( { a => 2, b => 1, n => 2 } );
        $coll->insert_one( { a => 3, b => 1, n => 3 } );
        $coll->insert_one( { a => 0, b => 1, n => 4 } );
        $coll->insert_one( { a => 1, b => 2, n => 5 } );
        $coll->insert_one( { a => 2, b => 3, n => 6 } );

        my @docs = $coll->find( { '$where' => $criteria } )->sort( { n => 1 } )->all;
        is( scalar @docs, 2, "correct count a > b" )
          or diag explain @docs;
        cmp_deeply(
            \@docs,
            [
                { _id => ignore(), a => num(2), b => num(1), n => num(2) },
                { _id => ignore(), a => num(3), b => num(1), n => num(3) }
            ],
            "javascript query correct"
        );
    };
}

subtest "sort standard hash" => sub {

  $coll->drop;

  $coll->insert_many( [
    { _id => 1, size => 10 },
    { _id => 2, size => 5  },
    { _id => 3, size => 15 },
  ] );

  my @res = $coll->find( {}, { sort => { size => 1 } } )->result->all;

  cmp_deeply \@res,
    [
      { _id => 2, size => 5  },
      { _id => 1, size => 10 },
      { _id => 3, size => 15 },
    ],
    'Got correct sort order';
};

subtest "sort BSON::Doc" => sub {

  $coll->drop;

  $coll->insert_many( [
    { _id => 1, size => 10 },
    { _id => 2, size => 5  },
    { _id => 3, size => 15 },
  ] );

  my $b_doc = bson_doc( size => 1 );
  my @res = $coll->find( {}, { sort => $b_doc } )->result->all;

  cmp_deeply \@res,
    [
      { _id => 2, size => 5  },
      { _id => 1, size => 10 },
      { _id => 3, size => 15 },
    ],
    'Got correct sort order';
};

subtest 'hint coercion' => sub {
  subtest "no hint" => sub {
    my $index_name = test_hints_setup();
    test_hints( $index_name );
  };

  subtest "hint string" => sub {
    my $index_name = test_hints_setup();
    test_hints( $index_name, $index_name );
  };

  subtest "hint array" => sub {
    my $index_name = test_hints_setup();
    test_hints( $index_name, [ qty => 1, category => 1 ] );
  };

  subtest "hint IxHash" => sub {
    my $index_name = test_hints_setup();
    test_hints( $index_name, Tie::IxHash->new( qty => 1, category => 1 ) );
  };

  subtest "hint BSON::Doc" => sub {
    my $index_name = test_hints_setup();
    test_hints( $index_name, bson_doc( qty => 1, category => 1 ) );
  };
};

sub test_hints {
  test_hints_aggregate( @_ );
  test_hints_count_documents( @_ );
  test_hints_find( @_ );
  test_hints_cursor( @_ );
}

sub test_hints_setup {

  $coll->drop;

  $coll->insert_many( [
    { _id => 1, category => "cake", type => "chocolate",    qty => 10 },
    { _id => 2, category => "cake", type => "ice cream",    qty => 25 },
    { _id => 3, category => "pie",  type => "boston cream", qty => 20 },
    { _id => 4, category => "pie",  type => "blueberry",    qty => 15 },
  ] );

  $coll->indexes->create_one( [ qty => 1, type => 1 ] );
  my $index_name = $coll->indexes->create_one( [qty => 1, category => 1 ] );

  return $index_name;
}

sub test_hints_aggregate {
  my ( $index_name, $hint ) = @_;

  subtest 'aggregate' => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    my $cursor = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } } ],
        { ( defined $hint ? ( hint => $hint ) : () ), explain => 1 }
    );

    my $result = $cursor->next;

    is( ref( $result ), 'HASH', "aggregate with explain returns a hashref" );

    if ( defined $hint ) {
      ok(
          scalar( @{ $result->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) == 0,
          "aggregate with hint had no rejectedPlans",
      );
    } else {
      ok(
          scalar( @{ $result->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) > 0,
          "aggregate with no hint had rejectedPlans",
      );
    }
  };
}

sub test_hints_count_documents {
  my ( $index_name, $hint ) = @_;

  subtest 'count_document' => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    is(
      $coll->count_documents(
        { category => 'cake', qty => { '$gt' => 0 } },
        { ( defined $hint ? ( hint => $hint ) : () ) } ),
      2,
      'count w/ spec' );
    is(
      $coll->count_documents(
        {},
        { ( defined $hint ? ( hint => $hint ) : () ) } ),
      4,
      'count' );
  };
}

sub test_hints_find {
  my ( $index_name, $hint ) = @_;

  subtest 'find' => sub {
    my $cursor = $coll->find(
      { category => 'cake', qty => { '$gt' => 15 } },
      { ( defined $hint ? ( hint => $hint ) : () ) }
    );

    my @res = $cursor->all;

    cmp_deeply \@res,
      [
        {
          _id => 2,
          category => "cake",
          qty => 25,
          type => "ice cream",
        },
      ],
      'Got correct result';
  }
}

sub test_hints_cursor {
  my ( $index_name, $hint ) = @_;

  subtest 'cursor' => sub {
    # Actually the same as find, just setting the hint after the fact
    my $cursor = $coll->find(
      { category => 'cake', qty => { '$gt' => 15 } }
    );

    $cursor->hint( $hint ) if defined $hint;

    my @res = $cursor->all;

    cmp_deeply \@res,
      [
        {
          _id => 2,
          category => "cake",
          qty => 25,
          type => "ice cream",
        },
      ],
      'Got correct result';
  }
}

done_testing;
