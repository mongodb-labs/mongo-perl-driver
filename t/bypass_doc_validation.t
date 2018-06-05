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

use utf8;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest
  qw/skip_unless_mongod build_client get_test_db server_version server_type get_capped/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_validating');

my $res;

my $does_validation = $server_version >= v3.1.3;

# only set up validation on servers that support it
sub _drop_coll {
    $coll->drop;
    $testdb->run_command( [ create => $coll->name ] );
    if ($does_validation) {
        $testdb->run_command(
            [ collMod => $coll->name, validator => { x => { '$exists' => 1 } } ] );
    }
    pass("reset collection");
}

subtest "insert_one" => sub {

    _drop_coll();

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->insert_one( {} ) },
            qr/failed validation/,
            "invalid insert_one throws error"
        );
    }

    is( exception { $coll->insert_one( {}, { bypassDocumentValidation => 1 } ) },
        undef, "validation bypassed" );

};

subtest "replace_one" => sub {

    _drop_coll();

    my $id = $coll->insert_one( { x => 1 } )->inserted_id;

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->replace_one( { _id => $id }, { y => 1 } ) },
            qr/failed validation/,
            "invalid replace_one throws error"
        );

    }

    is(
        exception {
            $coll->replace_one( { _id => $id }, { y => 1 }, { bypassDocumentValidation => 1 } )
        },
        undef,
        "validation bypassed"
    );

};

subtest "update_one" => sub {

    _drop_coll();

    $coll->insert_one( { x => 1 } );

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->update_one( { x => 1 }, { '$unset' => { x => 1 } } ) },
            qr/failed validation/,
            "invalid update_one throws error"
        );
    }

    is(
        exception {
            $coll->update_one(
                { x                        => 1 },
                { '$unset'                 => { x => 1 } },
                { bypassDocumentValidation => 1 }
              )
        },
        undef,
        "validation bypassed"
    );

};

subtest "update_many" => sub {

    _drop_coll();

    $coll->insert_many( [ { x => 1 }, { x => 2 } ] );

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->update_many( {}, { '$unset' => { x => 1 } } ) },
            qr/failed validation/,
            "invalid update_many throws error"
        );
    }

    is(
        exception {
            $coll->update_many(
                {},
                { '$unset'                 => { x => 1 } },
                { bypassDocumentValidation => 1 }
              )
        },
        undef,
        "validation bypassed"
    );

};

subtest 'bulk_write (unordered)' => sub {

    _drop_coll();

    my $err;

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        $err = exception {
            $coll->bulk_write(
                [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
                { ordered => 0 } );
        };

        like( $err, qr/failed validation/, "invalid bulk_write throws error" );
    }

    $err = exception {
        $coll->bulk_write(
            [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
            { bypassDocumentValidation => 1, ordered => 0 },
        );
    };

    is( $err, undef, "validation bypassed" );
};

subtest 'bulk_write (ordered)' => sub {

    _drop_coll();

    my $err;

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        $err = exception {
            $coll->bulk_write(
                [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
                { ordered => 1 } );
        };

        like( $err, qr/failed validation/, "invalid bulk_write throws error" );
    }

    $err = exception {
        $coll->bulk_write(
            [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
            { bypassDocumentValidation => 1, ordered => 1 },
        );
    };

    is( $err, undef, "validation bypassed" );
};

# insert_many uses bulk_write internally
subtest "insert_many" => sub {

    _drop_coll();

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->insert_many( [ {}, {} ] ) },
            qr/failed validation/,
            "invalid insert_many throws error"
        );
    }

    is(
        exception { $coll->insert_many( [ {}, {} ], { bypassDocumentValidation => 1 } ) },
        undef, "validation bypassed" );

};

subtest "find_one_and_replace" => sub {

    _drop_coll();

    $coll->insert_one( { x => 1 } );

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->find_one_and_replace( { x => 1 }, { y => 1 } ) },
            qr/failed validation/,
            "invalid find_one_and_replace throws error"
        );
    }

    is(
        exception {
            $coll->find_one_and_replace( { x => 1 }, { y => 1 }, { bypassDocumentValidation => 1 } )
        },
        undef,
        "validation bypassed"
    );

};

subtest "find_one_and_update" => sub {

    _drop_coll();

    $coll->insert_one( { x => 1 } );

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception { $coll->find_one_and_update( { x => 1 }, { '$unset' => { x => 1 } } ) },
            qr/failed validation/,
            "invalid find_one_and_update throws error"
        );
    }

    is(
        exception {
            $coll->find_one_and_update(
                { x                        => 1 },
                { '$unset'                 => { x => 1 } },
                { bypassDocumentValidation => 1 }
              )
        },
        undef,
        "validation bypassed"
    );

};

subtest "aggregate with \$out" => sub {
    plan skip_all => "Aggregation with \$out requires MongoDB 2.6+"
        unless $server_version >= v2.6.0;

    _drop_coll();

    my $source = $testdb->get_collection('test_source');
    $source->insert_many( [ map { { count => $_ } } 1 .. 20 ] );

    SKIP: {
        skip "without MongoDB 3.2+", 1 unless $does_validation;

        like(
            exception {
                $source->aggregate(
                    [ { '$match' => { count => { '$gt' => 10 } } }, { '$out' => $coll->name } ] );
            },
            qr/failed validation/,
            "invalid aggregate output throws error"
        );

        is( $coll->count, 0, "no docs in \$out collection" );
    }

    is(
        exception {
            $source->aggregate(
                [ { '$match' => { count => { '$gt' => 10 } } }, { '$out' => $coll->name } ],
                { bypassDocumentValidation => 1 } );
        },
        undef,
        "validation bypassed"
    );

    is( $coll->count, 10, "correct doc count in \$out collection" );

    is(
        exception {
            $source->aggregate(
                [ { '$match' => { count => { '$gt' => 10 } } } ],
                { bypassDocumentValidation => 1 } );
        },
        undef,
        "bypassDocumentValidation without \$out",
    );

};

done_testing;
