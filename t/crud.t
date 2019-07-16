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

use utf8;
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

my $supports_collation = $server_version >= 3.3.9;
my $case_insensitive_collation = { locale => "en_US", strength => 2 };

my $res;

subtest "insert_one" => sub {

    # insert doc with _id
    $coll->drop;
    $res = $coll->insert_one( { _id => "foo", value => "bar" } );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => str("foo"), value => str("bar") } ),
        "insert with _id: doc inserted"
    );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::InsertOneResult", "result" );
    is( $res->inserted_id, "foo", "res->inserted_id" );

    # insert doc without _id
    $coll->drop;
    my $orig = { value => "bar" };
    my $doc = { %$orig };
    $res = $coll->insert_one( $doc );
    my @got = $coll->find( {} )->all;
    cmp_deeply(
        \@got,
        bag( { _id => ignore(), value => str("bar") } ),
        "insert without _id: hash doc inserted"
    );
    ok( $res->acknowledged, "result acknowledged" );
    is( $got[0]{_id}, $res->inserted_id, "doc has expected inserted _id" );
    cmp_deeply( $doc, $orig, "original unmodified" );

    # insert arrayref
    $coll->drop;
    $res = $coll->insert_one( [ value => "bar" ] );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), value => str("bar") } ),
        "insert without _id: array doc inserted"
    );

    # insert Tie::Ixhash
    $coll->drop;
    $res = $coll->insert_one( Tie::IxHash->new( value => "bar" ) );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), value => str("bar") } ),
        "insert without _id: Tie::IxHash doc inserted"
    );

};

subtest "insert_many" => sub {

    # insert docs with mixed _id and not and mixed types
    $coll->drop;
    my $doc = { value => "baz" };
    $res =
      $coll->insert_many( [ [ _id => "foo", value => "bar" ], $doc, ] );
    my @got = $coll->find( {} )->all;
    cmp_deeply(
        \@got,
        bag( { _id => str("foo"), value => str("bar") }, { _id => ignore(), value => str("baz") }, ),
        "insert many: docs inserted"
    );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::InsertManyResult", "result" );
    cmp_deeply(
        $res->inserted,
        [ { index => num(0), _id => str("foo") }, { index => num(1), _id => obj_isa("BSON::OID") } ],
        "inserted contains correct hashrefs"
    );
    cmp_deeply(
        $res->inserted_ids,
        {
            0 => str("foo"),
            1 => $res->inserted->[1]{_id},
        },
        "inserted_ids contains correct keys/values"
    );
    is($res->inserted_count, 2, "Two docs inserted.");

    # ordered insert should halt on error
    $coll->drop;
    my $err = exception {
        $coll->insert_many( [ { _id => 0 }, { _id => 1 }, { _id => 2 }, { _id => 1 }, ] )
    };
    ok( $err, "ordered insert got an error" );
    isa_ok( $err, 'MongoDB::DuplicateKeyError', 'caught error' )
      or diag explain $err;
    $res = $err->result;
    is( $res->inserted_count, 3, "only first three inserted" );

    # unordered insert should not halt on error
    $coll->drop;
    $err = exception {
        $coll->insert_many( [ { _id => 0 }, { _id => 1 }, { _id => 1 }, { _id => 2 }, ], { ordered => 0 } )
    };
    ok( $err, "unordered insert got an error" );
    isa_ok( $err, 'MongoDB::DuplicateKeyError', 'caught error' )
      or diag explain $err;
    $res = $err->result;
    is( $res->inserted_count, 3, "all valid docs inserted" );

    # insert bad type
    $err = exception { $coll->insert_many( { x => 1 } ) };
    like( $err, qr/must be an array reference/, "exception inserting bad type" );
};

subtest "delete_one" => sub {
    $coll->drop;
    $coll->insert_many( [ map { { _id => $_, x => "foo" } } 1 .. 2 ] );
    is( $coll->count_documents( { x => 'foo' } ), 2, "inserted two docs" );
    $res = $coll->delete_one( { x => 'foo' } );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::DeleteResult", "result" );
    is( $res->deleted_count, 1, "delete one document" );
    is( $coll->count_documents( { x => 'foo' } ), 1, "one document left" );
    $res = $coll->delete_one( { x => 'bar' } );
    is( $res->deleted_count, 0, "delete non existent document does nothing" );
    is( $coll->count_documents( { x => 'foo' } ), 1, "one document left" );

    if ($supports_collation) {
        my $doc;
        $doc = $coll->find_one( { x => "foo" } );
        $res =
          $coll->delete_one( { x => 'FOO' }, { collation => $case_insensitive_collation } );
        is( $res->deleted_count, 1, "delete_one with collation" );
        is( $coll->count_documents( { x => 'foo' } ), 0, "no documents left" );

        my $coll2 = $coll->clone( write_concern => { w => 0 } );
        like(
            exception {
                $coll2->delete_one( { x => 'FOO' }, { collation => $case_insensitive_collation } );
            },
            qr/Unacknowledged deletes that specify a collation are not allowed/,
            "delete_one w/ collation returns error if write is unacknowledged"
        );
    }
    else {
        like(
            exception {
              $coll->delete_one( { x => 'FOO' }, { collation => $case_insensitive_collation } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "delete_one w/ collation returns error if unsupported"
        );
    }

    # test errors -- deletion invalid on capped collection
    my $cap = get_capped($testdb);
    $cap->insert_many( [ map { { _id => $_ } } 1..10 ] );
    my $err = exception { $cap->delete_one( { _id => 4 } ) };
    ok( $err, "deleting from capped collection throws error" );
    isa_ok( $err, 'MongoDB::WriteError' );
    like( $err->result->last_errmsg, qr/capped/, "error had string 'capped'" );
};

subtest "delete_many" => sub {
    $coll->drop;
    $coll->insert_many( [ map { { _id => $_, x => "foo" } } 1 .. 5 ] );
    is( $coll->count_documents( {} ), 5, "inserted five docs" );
    $res = $coll->delete_many( { _id => { '$gt', 3 } } );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::DeleteResult", "result" );
    is( $res->deleted_count, 2, "deleted two documents" );
    is( $coll->count_documents( {} ), 3, "three documents left" );
    $res = $coll->delete_many( { y => 'bar' } );
    is( $res->deleted_count, 0, "delete non existent document does nothing" );
    is( $coll->count_documents( {} ), 3, "three documents left" );

    if ($supports_collation) {
        $res =
          $coll->delete_many( { x => 'FOO' }, { collation => $case_insensitive_collation } );
        is( $res->deleted_count, 3, "delete_many with collation" );
        is( $coll->count_documents( {} ), 0, "no documents left" );
    }
    else {
        like(
            exception {
                $coll->delete_many( { x => 'FOO' }, { collation => $case_insensitive_collation } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "delete_many w/ collation returns error if unsupported"
        );
    }

    # test errors -- deletion invalid on capped collection
    my $cap = get_capped($testdb);
    $cap->insert_many( [ map { { _id => $_ } } 1..10 ] );
    my $err = exception { $cap->delete_many( {} ) };
    ok( $err, "deleting from capped collection throws error" );
    isa_ok( $err, 'MongoDB::WriteError' );
    like( $err->result->last_errmsg, qr/capped/, "error had string 'capped'" );
};

subtest "replace_one" => sub {
    $coll->drop;

    # replace missing doc without upsert
    $res = $coll->replace_one( { x => 1 }, { x => 2 } );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::UpdateResult", "result" );
    is( $res->matched_count, 0, "matched count is zero" );
    is( $coll->count_documents( {} ), 0, "collection still empty" );

    # replace missing with upsert
    $res = $coll->replace_one( { x => 1 }, { x => 2 }, { upsert => 1 } );
    is( $res->matched_count, 0, "matched count is zero" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 0 : undef ),
        "modified count correct based on server version"
    );

    ok( $server_version >= 2.6.0 ? $res->has_modified_count : !$res->has_modified_count,
        "has_modified_count correct" );

    isa_ok( $res->upserted_id, "BSON::OID", "got upserted id" );
    is( $coll->count_documents( {} ), 1, "one doc in database" );
    my $got = $coll->find_one( { _id => $res->upserted_id } );
    is( $got->{x}, 2, "document contents correct" );

    # replace existing with upsert -- add duplicate to confirm only one
    $coll->insert_one( { x => 2 } );
    $res = $coll->replace_one( { x => 2 }, { x => 3 }, { upsert => 1 } );
    is( $coll->count_documents( {} ), 2, "replace existing with upsert" );
    is( $res->matched_count, 1, "matched_count 1" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 1 : undef ),
        "modified count correct based on server version"
    );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(2) }, { _id => ignore, x => num(3) } ),
        "collection docs correct"
    );

    # replace existing without upsert
    $res = $coll->replace_one( { x => 3 }, { x => 4 } );
    is( $coll->count_documents( {} ), 2, "replace existing with upsert" );
    is( $res->matched_count, 1, "matched_count 1" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 1 : undef ),
        "modified count correct based on server version"
    );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(2) }, { _id => ignore, x => num(4) } ),
        "collection docs correct"
    );

    # replace doc with $op is an error
    my $err = exception {
        $coll->replace_one( { x => 3} , { '$set' => { x => 4 } } )
    };
    ok( $err, "replace with update operators is an error" );
    like( $err, qr/must not contain update operators/, "correct error message" );

    # replace doc with custom op_char is an error
    $err = exception {
        my $coll2 = $coll->with_codec( op_char => '-' );
        $coll2->replace_one( { x => 3} , { -set => { x => 4 } } )
    };
    ok( $err, "replace with op_char update operators is an error" );
    like( $err, qr/must not contain update operators/, "correct error message" );

    if ($supports_collation) {
        $coll->insert_one( { x => 'foo' } );

        $res = $coll->replace_one(
            { x         => 'FOO' },
            { x         => 'bar' },
            { collation => $case_insensitive_collation }
        );
        is( $coll->count_documents( { x => 'bar' } ), 1, "replace_one with collation" );
    }
    else {
        like(
            exception {
                $coll->replace_one(
                    { x         => 'FOO' },
                    { x         => 'bar' },
                    { collation => $case_insensitive_collation }
                );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "replace_one w/ collation returns error if unsupported"
        );
    }
};

subtest "update_one" => sub {
    $coll->drop;

    # update missing doc without upsert
    $res = $coll->update_one( { x => 1 }, { '$set' => { x => 2 } } );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::UpdateResult", "result" );
    is( $res->matched_count, 0, "matched count is zero" );
    is( $coll->count_documents( {} ), 0, "collection still empty" );

    # update missing with upsert
    $res = $coll->update_one( { x => 1 }, { '$set' => { x => 2 } }, { upsert => 1 } );
    is( $res->matched_count, 0, "matched count is zero" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 0 : undef ),
        "modified count correct based on server version"
    );
    isa_ok( $res->upserted_id, "BSON::OID", "got upserted id" );
    is( $coll->count_documents( {} ), 1, "one doc in database" );
    my $got = $coll->find_one( { _id => $res->upserted_id } );
    is( $got->{x}, 2, "document contents correct" );

    # update existing with upsert -- add duplicate to confirm only one
    $coll->insert_one( { x => 2 } );
    $res = $coll->update_one( { x => 2 }, { '$set' => { x => 3 } }, { upsert => 1 } );
    is( $coll->count_documents( {} ), 2, "update existing with upsert" );
    is( $res->matched_count, 1, "matched_count 1" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 1 : undef ),
        "modified count correct based on server version"
    );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(2) }, { _id => ignore, x => num(3) } ),
        "collection docs correct"
    );

    # update existing without upsert
    $res = $coll->update_one( { x => 3 }, { '$set' => { x => 4 } } );
    is( $coll->count_documents( {} ), 2, "update existing with upsert" );
    is( $res->matched_count, 1, "matched_count 1" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 1 : undef ),
        "modified count correct based on server version"
    );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(2) }, { _id => ignore, x => num(4) } ),
        "collection docs correct"
    );

    # update doc without $op is an error
    my $err = exception {
        $coll->update_one( { x => 3} , { x => 4 } )
    };
    ok( $err, "update without update operators is an error" );
    like( $err, qr/must only contain update operators/, "correct error message" );

    if ($supports_collation) {
        $coll->insert_one( { x => 'foo' } );

        $res = $coll->update_one(
            { x         => 'FOO' },
            { '$set'    => { x => 'bar' } },
            { collation => $case_insensitive_collation }
        );
        is( $coll->count_documents( { x => 'bar' } ), 1, "update_one with collation" );

        my $coll2 = $coll->clone( write_concern => { w => 0 } );
        like(
            exception {
                $coll2->update_one(
                    { x         => 'FOO' },
                    { '$set'    => { x => 'bar' } },
                    { collation => $case_insensitive_collation }
                );
            },
            qr/Unacknowledged updates that specify a collation are not allowed/,
            "update_one w/ collation returns error if write is unacknowledged"
        );
    }
    else {
        like(
            exception {
                $coll->update_one(
                    { x         => 'FOO' },
                    { '$set'    => { x => 'bar' } },
                    { collation => $case_insensitive_collation }
                );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "update_one w/ collation returns error if unsupported"
        );
    }
};

subtest "update_many" => sub {
    $coll->drop;

    # update missing doc without upsert
    $res = $coll->update_many( { x => 1 }, { '$set' => { x => 2 } } );
    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::UpdateResult", "result" );
    is( $res->matched_count, 0, "matched count is zero" );
    is( $coll->count_documents( {} ), 0, "collection still empty" );

    # update missing with upsert
    $res = $coll->update_many( { x => 1 }, { '$set' => { x => 2 } }, { upsert => 1 } );
    is( $res->matched_count, 0, "matched count is zero" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 0 : undef ),
        "modified count correct based on server version"
    );
    isa_ok( $res->upserted_id, "BSON::OID", "got upserted id" );
    is( $coll->count_documents( {} ), 1, "one doc in database" );
    my $got = $coll->find_one( { _id => $res->upserted_id } );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(2) } ),
        "collection docs correct"
    );

    # update existing with upsert -- add duplicate to confirm multiple
    $coll->insert_one( { x => 2 } );
    $res = $coll->update_many( { x => 2 }, { '$set' => { x => 3 } }, { upsert => 1 } );
    is( $coll->count_documents( {} ), 2, "update existing with upsert" );
    is( $res->matched_count, 2, "matched_count 2" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 2 : undef ),
        "modified count correct based on server version"
    );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(3) }, { _id => ignore, x => num(3) } ),
        "collection docs correct"
    );

    # update existing without upsert
    $res = $coll->update_many( { x => 3 }, { '$set' => { x => 4 } } );
    is( $coll->count_documents( {} ), 2, "update existing with upsert" );
    is( $res->matched_count, 2, "matched_count 1" );
    is(
        $res->modified_count,
        ( $server_version >= v2.6.0 ? 2 : undef ),
        "modified count correct based on server version"
    );
    cmp_deeply(
        [ $coll->find( {} )->all ],
        bag( { _id => ignore(), x => num(4) }, { _id => ignore, x => num(4) } ),
        "collection docs correct"
    );

    # update doc without $op is an error
    my $err = exception {
        $coll->update_one( { x => 3 } , { x => 4 } )
    };
    ok( $err, "update without update operators is an error" );
    like( $err, qr/must only contain update operators/, "correct error message" );

    if ($supports_collation) {
        $coll->insert_one( { x => 'foo' } );
        $coll->insert_one( { x => 'Foo' } );

        $res = $coll->update_many(
            { x         => 'FOO' },
            { '$set'    => { x => 'bar' } },
            { collation => $case_insensitive_collation }
        );
        is( $coll->count_documents( { x => 'bar' } ), 2, "update_many with collation" );
    }
    else {
        like(
            exception {
                $coll->update_many(
                    { x         => 'FOO' },
                    { '$set'    => { x => 'bar' } },
                    { collation => $case_insensitive_collation }
                );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "update_many w/ collation returns error if unsupported"
        );
    }
};

subtest 'bulk_write' => sub {
    $coll->drop;

    # test mixed-form write models, array/hash refs or pairs
    $res = $coll->bulk_write(
        [
            [ insert_one  => [ { x => 1 } ] ],
            { insert_many => [ { x => 2 }, { x => 3 } ] },
            replace_one => [ { x => 1 }, { x      => 4 } ],
            update_one  => [ { x => 7 }, { '$set' => { x => 5 } }, { upsert => 1 } ],
            [ insert_one  => [ { x => 6 } ] ],
            { insert_many => [ { x => 7 }, { x => 8 } ] },
            delete_one  => [ { x => 4 } ],
            delete_many => [ { x => { '$lt' => 3 } } ],
            update_many => [ { x => { '$gt' => 5 } }, { '$inc' => { x => 1 } } ],
        ],
    );

    ok( $res->acknowledged, "result acknowledged" );
    isa_ok( $res, "MongoDB::BulkWriteResult", "result" );
    is( $res->op_count, 11, "op count correct" );

    my @got = $coll->find( {} )->all;
    cmp_deeply(
        \@got,
        bag( map { { _id => ignore, x => num($_) } } 3, 5, 7, 8, 9 ),
        "collection docs correct",
    ) or diag explain \@got;

    # test ordered error
    # ordered insert should not halt on error
    $coll->drop;
    my $err = exception {
        $coll->bulk_write(
            [
                insert_one => [ { _id => 1 } ],
                insert_one => [ { _id => 2 } ],
                insert_one => [ { _id => 1 } ],
            ],
            { ordered => 1, },
        );
    };
    ok( $err, "ordered bulk got an error" );
    isa_ok( $err, 'MongoDB::DuplicateKeyError', 'caught error' )
      or diag explain $err;
    $res = $err->result;
    is( $res->inserted_count, 2, "only first two inserted" );

    # test unordered error
    # unordered insert should halt on error
    $coll->drop;
    $err = exception {
        $coll->bulk_write(
            [
                insert_one => [ { _id => 1 } ],
                insert_one => [ { _id => 2 } ],
                insert_one => [ { _id => 1 } ],
                insert_one => [ { _id => 3 } ],
            ],
            { ordered => 0, },
        );
    };
    ok( $err, "unordered bulk got an error" );
    isa_ok( $err, 'MongoDB::DuplicateKeyError', 'caught error' )
      or diag explain $err;
    $res = $err->result;
    is( $res->inserted_count, 3, "three valid docs inserted" );

    # test collation
    $coll->drop;
    $coll->insert_one( { x => $_, y => 1 } ) for "a" .. "e";
    $err = exception {
        $coll->bulk_write(
            [
                update_one => [
                    { x         => "A" },
                    { '$set'    => { y => 0 } },
                    { collation => $case_insensitive_collation }
                ],
                update_many => [
                    { x         => "B" },
                    { '$set'    => { y => 0 } },
                    { collation => $case_insensitive_collation }
                ],
                replace_one =>
                  [ { x => "C" }, { y => 0 }, { collation => $case_insensitive_collation } ],
                delete_one  => [ { x => "D" }, { collation => $case_insensitive_collation } ],
                delete_many => [ { x => "E" }, { collation => $case_insensitive_collation } ],
            ]
        );
    };
    if ($supports_collation) {
        is( $err, undef, "bulk_write w/ collation" );
        is( $coll->count_documents( { y => 1 } ), 0, "collection updated" );
    }
    else {
        like(
            $err,
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "bulk_write w/ collation returns error if unsupported"
        );
        is( $coll->count_documents( { y => 1 } ), 5, "collection not updated" );
    }
};

subtest "find_one_and_delete" => sub {
    $coll->drop;
    $coll->insert_one( { x => 1, y => 'a' } );
    $coll->insert_one( { x => 1, y => 'b' } );
    is( $coll->count_documents( {} ), 2, "inserted 2 docs" );

    my $doc;

    # find non-existent doc
    $doc = $coll->find_one_and_delete( { x => 2 } );
    is( $doc, undef, "find_one_and_delete on nonexistent doc returns undef" );
    is( $coll->count_documents( {} ), 2, "still 2 docs" );

    # find/remove existing doc (testing sort and projection, too)
    $doc = $coll->find_one_and_delete( { x => 1 },
        { sort => [ y => 1 ], projection => { y => 1 } } );
    cmp_deeply( $doc, { _id => ignore(), y => str("a") }, "expected doc returned" );
    is( $coll->count_documents( {} ), 1, "only 1 doc left" );

    if ($supports_collation) {
        $doc = $coll->find_one_and_delete(
            { y         => 'B' },
            { collation => $case_insensitive_collation },
        );
        cmp_deeply(
            $doc,
            { _id => ignore(), x => num(1), y => str("b") },
            "find_one_and_delete with collation"
        );
        is( $coll->count_documents( { y => 'b' } ), 0, "no documents left" );
    }
    else {
        like(
            exception {
                $coll->find_one_and_delete(
                    { y         => 'B' },
                    { collation => $case_insensitive_collation },
                );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "find_one_and_delete w/ collation returns error if unsupported"
        );
    }

    # XXX how to test max_time_ms?
};

subtest "find_one_and_replace" => sub {
    $coll->drop;
    $coll->insert_one( { x => 1, y => 'a' } );
    $coll->insert_one( { x => 1, y => 'b' } );
    is( $coll->count_documents( {} ), 2, "inserted 2 docs" );

    my $doc;

    # find and replace non-existent doc, without upsert
    $doc = $coll->find_one_and_replace( { x => 2 }, { x => 3, y => 'c' } );
    is( $doc, undef, "find_one_and_replace on nonexistent doc returns undef" );
    is( $coll->count_documents( {} ), 2, "still 2 docs" );
    is( $coll->count_documents( { x => 3 } ), 0, "no docs matching replacment" );

    # find and replace non-existent doc, with upsert
    $doc = $coll->find_one_and_replace( { x => 2 }, { x => 3, y => 'c' }, { upsert => 1 } );
    unless ( check_min_server_version($conn, 'v2.2.0') ) {
        is( $doc, undef, "find_one_and_replace upsert on nonexistent doc returns undef" );
    }
    is( $coll->count_documents( {} ), 3, "doc has been upserted" );
    is( $coll->count_documents( { x => 3 } ), 1, "1 doc matching replacment" );

    # find and replace existing doc, with upsert
    $doc = $coll->find_one_and_replace( { x => 3 }, { x => 4, y => 'c' }, { upsert => 1 });
    cmp_deeply(
        $doc,
        { _id => ignore(), x => num(3), y => str("c") },
        "find_one_and_replace on existing doc returned old doc",
    );
    is( $coll->count_documents( {} ), 3, "no new doc added" );
    is( $coll->count_documents( { x => 4 } ), 1, "1 doc matching replacment" );

    # find and replace existing doc, with after doc
    $doc = $coll->find_one_and_replace( { x => 4 }, { x => 5, y => 'c' }, { returnDocument => 'after' });
    cmp_deeply(
        $doc,
        { _id => ignore(), x => num(5), y => str("c") },
        "find_one_and_replace on existing doc returned new doc",
    );
    is( $coll->count_documents( {} ), 3, "no new doc added" );
    is( $coll->count_documents( { x => 5 } ), 1, "1 doc matching replacment" );

    # test project and sort
    $doc = $coll->find_one_and_replace( { x => 1 }, { x => 2, y => 'z' }, { sort => [ y => -1 ], projection => { y => 1 } } );
    cmp_deeply(
        $doc,
        { _id => ignore(), y => str("b") },
        "find_one_and_replace on existing doc returned new doc",
    );
    is( $coll->count_documents( { x => 2 } ), 1, "1 doc matching replacment" );
    is( $coll->count_documents( { x => 1, y => 'a' } ), 1, "correct doc untouched" );

    # test duplicate key error
    $coll->drop;
    $coll->insert_many( [ map { { _id => $_ } } 0 .. 2 ] );
    my $err = exception {
        $coll->find_one_and_replace( { x => 1 }, { _id => 0 }, { upsert => 1 } );
    };
    ok( $err, "upsert dup key got an error" );
    isa_ok( $err, 'MongoDB::DuplicateKeyError', 'caught error' )
      or diag explain $err;

    $coll->drop;
    if ($supports_collation) {
        $coll->insert_one( { y => 'b' } );

        $doc = $coll->find_one_and_replace(
            { y         => 'B' },
            { y         => 'c' },
            { collation => $case_insensitive_collation },
        );
        cmp_deeply(
            $doc,
            { _id => ignore(), y => str("b") },
            "find_one_and_replace with collation"
        );
        is( $coll->count_documents( { y => 'c' } ), 1, "doc matching replacement" );
    }
    else {
        like(
            exception {
                $coll->find_one_and_replace(
                    { y         => 'B' },
                    { y         => 'c' },
                    { collation => $case_insensitive_collation },
                );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "find_one_and_replace w/ collation returns error if unsupported"
        );
    }
};

subtest "find_one_and_update" => sub {
    $coll->drop;
    $coll->insert_one( { x => 1, y => 'a' } );
    $coll->insert_one( { x => 1, y => 'b' } );
    is( $coll->count_documents( {} ), 2, "inserted 2 docs" );

    my $doc;

    # find and update non-existent doc, without upsert
    $doc = $coll->find_one_and_update( { x => 2 }, { '$inc' => { x => 1 } } );
    is( $doc, undef, "find_one_and_update on nonexistent doc returns undef" );
    is( $coll->count_documents( {} ), 2, "still 2 docs" );
    is( $coll->count_documents( { x => 3 } ), 0, "no docs matching update" );

    # find and update non-existent doc, with upsert
    $doc = $coll->find_one_and_update( { x => 2 }, { '$inc' => { x => 1 }, '$set' => { y => 'c' } }, { upsert => 1 } );
    unless ( check_min_server_version($conn, 'v2.2.0') ) {
        is( $doc, undef, "find_one_and_update upsert on nonexistent doc returns undef" );
    }
    is( $coll->count_documents( {} ), 3, "doc has been upserted" );
    is( $coll->count_documents( { x => 3 } ), 1, "1 doc matching upsert" );

    # find and update existing doc, with upsert
    $doc = $coll->find_one_and_update( { x => 3 }, { '$inc' => { x => 1 } }, { upsert => 1 });
    cmp_deeply(
        $doc,
        { _id => ignore(), x => num(3), y => str("c") },
        "find_one_and_update on existing doc returned old doc",
    );
    is( $coll->count_documents( {} ), 3, "no new doc added" );
    is( $coll->count_documents( { x => 4 } ), 1, "1 doc matching replacment" );

    # find and update existing doc, with after doc
    $doc = $coll->find_one_and_update( { x => 4 }, { '$inc' => { x => 1 } }, { returnDocument => 'after' });
    cmp_deeply(
        $doc,
        { _id => ignore(), x => num(5), y => str("c") },
        "find_one_and_update on existing doc returned new doc",
    );
    is( $coll->count_documents( {} ), 3, "no new doc added" );
    is( $coll->count_documents( { x => 5 } ), 1, "1 doc matching replacment" );

    # test project and sort
    $doc = $coll->find_one_and_update(
        { x      => 1 },
        { '$inc' => { x => 1 }, '$set' => { y => 'z' } },
        { sort   => [ y => -1 ], projection => { y => 1 } }
    );
    cmp_deeply(
        $doc,
        { _id => ignore(), y => str("b") },
        "find_one_and_update on existing doc returned new doc",
    );
    is( $coll->count_documents( { x => 2 } ), 1, "1 doc matching replacment" );
    is( $coll->count_documents( { x => 1, y => 'a' } ), 1, "correct doc untouched" );

    # test duplicate key error
    $coll->drop;
    $coll->indexes->create_one([x => 1], {unique => 1});
    $coll->insert_many( [ map { { _id => $_, x => $_ } } 1 .. 3 ] );
    my $err = exception {
        $coll->find_one_and_update( { x => 0 }, { '$set' => { x => 1 } }, { upsert => 1 } );
    };
    ok( $err, "update dup key got an error" );
    isa_ok( $err, 'MongoDB::DuplicateKeyError', 'caught error' )
      or diag explain $err;

    $coll->drop;
    if ($supports_collation) {
        $coll->insert_one( { y => 'b' } );

        $doc = $coll->find_one_and_update(
            { y         => 'B' },
            { '$set'    => { y => 'c' } },
            { collation => $case_insensitive_collation },
        );
        cmp_deeply(
            $doc,
            { _id => ignore(), y => str("b") },
            "find_one_and_update with collation"
        );
        is( $coll->count_documents( { y => 'c' } ), 1, "doc matching replacement" );
    }
    else {
        like(
            exception {
                $coll->find_one_and_update(
                    { y         => 'B' },
                    { '$set'    => { y => 'c' } },
                    { collation => $case_insensitive_collation },
                );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "find_one_and_update w/ collation returns error if unsupported"
        );
    }
};

subtest "write concern errors" => sub {
    plan skip_all => "not a replica set"
        unless $server_type eq 'RSPrimary';

    $coll->drop;
    my $coll2 = $coll->clone( write_concern => { w => 99 } );

    my @cases = (
        [ insert_one => [ { x => 1 } ] ],
        [ insert_many => [ [ { x => 2 }, { x => 3 } ] ] ],
        [ delete_one => [ { x => 1 } ] ],
        [ delete_one => [ {} ] ],
        [ replace_one => [ { x => 0 }, { x => 1 }, { upsert => 1 } ] ],
        [ update_one => [ { x => 1 }, { '$inc' => { x => 1 } } ] ],
    );

    # findAndModify doesn't take write concern until MongoDB 3.2
    unless ( check_min_server_version($conn, 'v3.2.0') ) {
        push @cases,
          (
            [ find_one_and_replace => [ { x => 2 }, { x      => 1 } ] ],
            [ find_one_and_update  => [ { x => 1 }, { '$inc' => { x => 1 } } ] ],
            [ find_one_and_delete => [ { x => 2 } ] ],
          );
    }

    for my $c ( @cases ) {
        my ($method, $args) = @$c;
        my $res;
        my $err = exception { $res = $coll2->$method( @$args ) };
        ok( $err, "caught error for $method" ) or diag explain $res;
        isa_ok( $err, 'MongoDB::WriteConcernError', "$method error" )
            or diag explain $err;
    }
};


done_testing;
