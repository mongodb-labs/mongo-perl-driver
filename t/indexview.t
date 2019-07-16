#  Copyright 2015 - present MongoDB, Inc.
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

use Tie::IxHash;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    get_capped
    check_min_server_version
    skip_unless_min_version
/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');
my $admin          = $conn->get_database("admin");

my $supports_collation = $server_version >= 3.3.9;
my $valid_collation           = { locale => "en_US", strength => 2 };
my $valid_collation_alternate = { locale => "fr_CA" };
my $invalid_collation         = { locale => "en_US", blah => 5 };

my $supports_index_allpaths = $server_version >= v4.1.5;

my ($iv);

# XXX work around SERVER-18062; create collection to initialize DB for
# sharded collection so gridfs index creation doesn't fail
$testdb->coll("testtesttest")->insert_one({});

subtest "collection API" => sub {
    $iv = $coll->indexes;
    isa_ok( $iv, "MongoDB::IndexView", "coll->indexes" );
};

subtest "create_many" => sub {
    $coll->drop;
    my @names = $iv->create_many( { keys => [ x => 1 ] }, { keys => [ y => -1 ] } );
    ok( scalar @names, "got non-empty result" );
    is_deeply( [ sort @names ], [ sort qw/x_1 y_-1/ ], "returned list of names" );

    # exception on index creation
    SKIP: {
        skip "bad index type won't fail before 2.4", 1
            if $server_version <= v2.4.0;
        like(
            exception {
                $iv->create_many( { keys => [ x => '4d' ] } );
            },
            qr/MongoDB::(?:Database|Write)Error/,
            "exception creating impossible index",
        );
    }

    like(
        exception { $iv->create_many( { keys => { x => 1, y => 1 } } ) },
        qr/index models/,
        "exception giving unordered docs for keys"
    );

    is( exception { $iv->create_many( { keys => { y => 1 } } ) },
        undef, "no exception on single-key hashref" );

    $coll->drop;
    if ($supports_collation) {
        ok(
            $iv->create_many(
                { keys => { x => 1 } },
                { keys => { y => 1 }, options => { collation => $valid_collation } }
            ),
            "create_many with valid collation"
        );

        my @indexes = grep { $_->{name} eq "y_1" } $iv->list->all;
        is( 1, scalar @indexes, "index created successfully" );
        my $index = $indexes[0];
        is( $index->{collation}{locale},   "en_US", "created index has correct locale" );
        is( $index->{collation}{strength}, 2,       "created index has correct strength" );
    }
    else {
        like(
            exception {
                $iv->create_many( { keys => { x => 1 } },
                    { keys => { y => 1 }, options => { collation => $valid_collation } } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "create_many w/ collation returns error if unsupported"
        );
    }
};

subtest "list indexes" => sub {
    $coll->drop;
    $coll->insert_one( {} );
    my $res = $iv->list();
    isa_ok( $res, "MongoDB::QueryResult", "indexes->list" );
    is_deeply( [ sort map { $_->{name} } $res->all ],
        ['_id_'], "list only gives _id_ index" );
    ok( $iv->create_many( { keys => [ x => 1 ] } ), "added index" );
    is_deeply(
        [ sort map { $_->{name} } $iv->list->all ],
        [ sort qw/_id_ x_1/ ],
        "list finds both indexes"
    );
};

subtest "create_one" => sub {
    $coll->drop;
    my $name = $iv->create_one( [ x => 1 ] );
    my $found = grep { $_->{name} eq 'x_1' } $iv->list->all;
    ok( $found, "created one index on x" );

    ok( $iv->create_one( [ y => -1 ], { unique => 1 } ), "created unique index on y" );
    ($found) = grep { $_->{name} eq 'y_-1' } $iv->list->all;
    ok( $found->{unique}, "saw unique property in index info for y" );

    like( exception { $iv->create_one( [ x => 1 ], { keys => [ y => 1 ] } ) },
        qr/MongoDB::UsageError/, "exception putting 'keys' in options" );

    like( exception { $iv->create_one( [ x => 1 ], { key => [ y => 1 ] } ) },
        qr/MongoDB::UsageError/, "exception putting 'key' in options" );

    like(
        exception { $iv->create_one( { x => 1, y => 1 } ) },
        qr/ordered document/,
        "exception giving unordered docs for keys"
    );

    is( exception { $iv->create_one( { y => 1 } ) },
        undef, "no exception on single-key hashref" );

    # exception on index creation
    SKIP: {
        skip "bad index type won't fail before 2.4", 1
            if $server_version <= v2.4.0;
        like(
            exception {
                $iv->create_one( [ x => '4d' ] );
            },
            qr/MongoDB::(?:Database|Write)Error/,
            "exception creating impossible index",
        );
    }

    $coll->drop;
    if ($supports_collation) {
        ok( $iv->create_one( { x => 1 }, { collation => $valid_collation } ),
            "create_one with valid collation" );

        $coll->drop;
        isnt(
            exception {
                $iv->create_one( { x => 1 }, { collation => $invalid_collation } );
            },
            undef,
            "create_one with invalid collation"
        );

        $coll->drop;
    }
    else {
        like(
            exception {
                $iv->create_one( { x => 1 }, { collation => $valid_collation } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "create_one w/ collation returns error if unsupported"
        );
    }
};

subtest "drop_one" => sub {
    $coll->drop;
    ok( my $name = $iv->create_one( [ x => 1 ] ), "created index on x" );
    my $res = $iv->drop_one($name);
    ok( $res->{ok}, "result of drop_one is a database result document" );
    my $found = grep { $_->{name} eq 'x_1' } $iv->list->all;
    ok( !$found, "dropped index on x" );

    # Dropping non-existing index must not error
    $res = $iv->drop_one("z_3");
    ok( !$res->{ok}, "result of drop_one is a database result document with false, but no exception" );

    # exception on index drop
    like(
        exception { $iv->drop_one("*") },
        qr/MongoDB::UsageError/,
        "exception calling drop_one on '*'"
    );

    like(
        exception {
            $iv->drop_one('_id_');
        },
        qr/MongoDB::(?:Database|Write)Error/,
        "exception dropping _id_",
    );

    like(
        exception {
            $iv->drop_one( { keys => [ x => 1 ] } );
        },
        qr/must be a string/,
        "exception dropping hashref"
    );
};

subtest "drop_all" => sub {
    $coll->drop;
    $iv->create_many( map { { keys => $_ } }[ x => 1 ], [ y => 1 ], [ z => 1 ] );
    is_deeply(
        [ sort map $_->{name}, $iv->list->all ],
        [ sort qw/_id_ x_1 y_1 z_1/ ],
        "created three indexes"
    );

    my $res = $iv->drop_all;
    ok( $res->{ok}, "result of drop_all is a database result document" );
    is_deeply( [ sort map $_->{name}, $iv->list->all ],
        [qw/_id_/], "dropped all but _id index" );

};

subtest 'handling duplicates' => sub {
    $coll->drop;
    my $doc = { foo => 1, bar => 1, baz => 1, boo => 1 };
    $coll->insert_one($doc) for 1 .. 2;
    is( $coll->count_documents({}), 2, "two identical docs inserted" );
    like( exception { $iv->create_one( [ foo => 1 ], { unique => 1 } ) },
        qr/E11000/, "got expected error creating unique index with dups" );

    # prior to 2.7.5, drop_dups was respected
    if ( check_min_server_version($conn, 'v2.7.5') ) {
        ok( $iv->create_one( [ foo => 1 ], { unique => 1, dropDups => 1 } ),
            "create unique with dropDups" );
        is( $coll->count_documents({}), 1, "one doc dropped" );
    }
};

subtest '2d index with options' => sub {
    $coll->drop;
    $iv->create_one( [ loc => '2d' ], { bits => 32, sparse => 1 } );
    my ($index) = grep { $_->{name} eq 'loc_2d' } $iv->list->all;
    ok( $index,           "created 2d index" );
    ok( $index->{sparse}, "sparse option set on index" );
    is( $index->{bits}, 32, "bits option set on index" );
};

subtest 'ensure index arbitrary options' => sub {
    $coll->drop;
    eval { $iv->create_one( { wibble => 1 }, { notReallyAnOption => { foo => 1 } } ); };
    # for invalid options, we expect either a server error or an index successfully
    # created with the requested option
    if ($@) {
        isa_ok( $@, "MongoDB::DatabaseError", "error from create_one w/ invalid opts" );
    }
    else {
        my ($index) = grep { $_->{name} eq 'wibble_1' } $iv->list->all;
        ok( $index, "created index" );
        cmp_deeply(
            $index->{notReallyAnOption},
            { foo => 1 },
            "arbitrary option set on index"
        );
    }
};

subtest 'indexes w/ same key pattern but different collations' => sub {
    plan skip_all => "Server version $server_version doesn't support collation"
      unless $supports_collation;

    $coll->drop;
    $iv->create_one( { a => 1 }, { collation => $valid_collation, name => "index1" } );
    $iv->create_one( { a => 1 },
        { collation => $valid_collation_alternate, name => "index2" } );
    cmp_deeply(
        [ map { $_->{key} } $iv->list->all ],
        [ { _id => num(1) }, { a => num(1) }, { a => num(1) } ],
        "both indexes created"
    );
    $iv->drop_one("index1");
    cmp_deeply(
        [ map { $_->{name} } $iv->list->all ],
        [ str("_id_"), str("index2") ],
        "correct index dropped"
    );
};

# test index names with "."s
subtest "index with dots" => sub {
    $coll->drop;
    $iv->create_one( { "x.y" => 1 }, { name => "foo" } );
    my ($index) = grep { $_->{name} eq 'foo' } $iv->list->all;
    ok( $index,                 "got index" );
    ok( $index->{key},          "has key field" );
    ok( $index->{key}->{'x.y'}, "has dotted field in key" );
    $coll->drop;
};

# sparse indexes
subtest "sparse indexes" => sub {
    $coll->drop;
    for ( 1 .. 10 ) {
        $coll->insert_one( { x => $_, y => $_ } );
        $coll->insert_one( { x => $_ } );
    }
    is( $coll->count_documents({}), 20, "inserted 20 docs" );

    like(
        exception { $iv->create_one( { y => 1 }, { unique => 1, name => "foo" } ) },
        qr/MongoDB::DuplicateKeyError/,
        "error creating non-sparse index"
    );
    my ($index) = grep { $_->{name} eq 'foo' } $iv->list->all;
    ok( !$index, "index not found" );

    $iv->create_one( { y => 1 }, { unique => 1, sparse => 1, name => "foo" } );
    ($index) = grep { $_->{name} eq 'foo' } $iv->list->all;
    ok( $index, "sparse index created" );
};

# text indices
subtest 'text indices' => sub {
    skip_unless_min_version($conn, 'v2.4.0');

    # parameter required only on 2.4; deprecated as of 2.6; removed for 3.4
    if ( check_min_server_version($conn, 'v2.6.0') ) {
        my $res = $conn->get_database('admin')
        ->run_command( [ 'getParameter' => 1, 'textSearchEnabled' => 1 ] );
        plan skip_all => "text search not enabled"
        if !$res->{'textSearchEnabled'};
    }

    my $coll2 = $testdb->get_collection('test_text');
    $coll2->drop;
    $coll2->insert_one( { language => 'english', w1 => 'hello', w2 => 'world' } )
      foreach ( 1 .. 10 );
    is( $coll2->count_documents({}), 10, "inserted 10 documents" );

    my $res = $coll2->indexes->create_one(
        { '$**' => 'text' },
        {
            name              => 'testTextIndex',
            default_language  => 'spanish',
            language_override => 'language',
            weights           => { w1 => 5, w2 => 10 }
        }
    );

    ok( $res, "created text index" );

    my ($text_index) = grep { $_->{name} eq 'testTextIndex' } $coll2->indexes->list->all;
    is( $text_index->{'default_language'}, 'spanish', 'default_language option works' );
    is( $text_index->{'language_override'},
        'language', 'language_override option works' );
    is( $text_index->{'weights'}->{'w1'}, 5,  'weights option works 1' );
    is( $text_index->{'weights'}->{'w2'}, 10, 'weights option works 2' );

    # 2.6 deprecated 'text' command and added '$text' operator; also the
    # result format changed.
    if ( $server_version >= v2.6.0 ) {
        my $n_found =()= $coll2->find( { '$text' => { '$search' => 'world' } } )->all;
        is( $n_found, 10, "correct number of results found" );
    }
    else {
        my $results =
          $testdb->run_command( [ 'text' => 'test_text', 'search' => 'world' ] )->{results};
        is( scalar(@$results), 10, "correct number of results found" );
    }
};

subtest 'index key order' => sub {
  $coll->drop;

  # Just need one insert to re-create the collection
  $coll->insert_one( { x => 1 } );

  my $index_1 = $iv->create_one([x => 1, y => 1]);
  my $index_2 = $iv->create_one([x => 1, z => 1]);
  my $index_3 = $iv->create_one([x => 1, y => 1, z => 1]);

  my $index_map = {
    $index_1 => [x => 1, y => 1],
    $index_2 => [x => 1, z => 1],
    $index_3 => [x => 1, y => 1, z => 1],
  };

  my @indices = $iv->list->all;

  for my $index ( @indices ) {
    next unless defined $index_map->{ $index->{name} };

    cmp_deeply
      [ %{ $index->{key} } ],
      $index_map->{ $index->{name} },
      'Key correct to name ' . $index->{name};
  }
};

subtest 'index all paths' => sub {
    plan skip_all => "Server version $server_version doesn't support index all paths"
      unless $supports_index_allpaths;
    $coll->drop;
    $iv->create_one( { '$**' => 1 }, { name => 'allpaths' } );
    foreach my $index ($iv->list->all) {
      next unless $index->{'name'} eq 'allpaths';
      ok($index->{'key'}{'$**'});
    }
};

done_testing;

# vim: set ts=4 sts=4 sw=4 et tw=75:
