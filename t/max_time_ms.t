#  Copyright 2014 - present MongoDB, Inc.
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
use version;
use Test::More 0.96;
use Test::Fatal;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw(
  skip_unless_mongod
  build_client
  get_test_db
  server_type
  server_version
  skip_unless_failpoints_available
  set_failpoint
  clear_failpoint
  check_min_server_version
  skip_unless_min_version
);

skip_unless_mongod();
skip_unless_failpoints_available();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_type    = server_type($conn);
my $server_version = server_version($conn);

my $coll;
my $admin = $conn->get_database("admin");

note "CAP-401 test plan";

can_ok( 'MongoDB::Cursor', 'max_time_ms' );

$coll = $testdb->get_collection("test_collection");

my $bulk = $coll->ordered_bulk;
$bulk->insert_one( { _id => $_ } ) for 1 .. 20;
my $err = exception { $bulk->execute };
is( $err, undef, "inserted 20 documents for testing" );

my $iv = $coll->indexes;

subtest "expected behaviors" => sub {

    is( exception { $coll->find->max_time_ms()->next },  undef, "find->max_time_ms()" );
    is( exception { $coll->find->max_time_ms(0)->next }, undef, "find->max_time_ms(0)" );
    is( exception { $coll->find->max_time_ms(5000)->next },
        undef, "find->max_time_ms(5000)" );

    like( exception { $coll->find->max_time_ms(-1)->next },
        qr/non-negative/, "find->max_time_ms(-1) throws exception" );

    is( exception { $coll->find( {}, { maxTimeMS => 5000 } ) },
        undef, "find with maxTimeMS" );

    is(
        exception {
            my $doc = $coll->find_one( { _id => 1 }, undef, { maxTimeMS => 5000 } );
        },
        undef,
        "find_one with maxTimeMS works"
    );

    SKIP: {
        skip "aggregate not available until MongoDB v2.2", 1
            if check_min_server_version($conn, 'v2.2.0');

        is(
            exception {
                my $doc = $coll->aggregate(
                    [ { '$project' => { name => 1, count => 1 } } ],
                    { maxTimeMS => 5000 },
                );
            },
            undef,
            "aggregate helper with maxTimeMS works"
        );
    }

    is(
        exception {
            my $doc = $coll->count_documents( {}, { maxTimeMS => 5000 } );
        },
        undef,
        "count helper with maxTimeMS works"
    );

    is(
        exception {
            my $doc = $coll->distinct( 'a', {}, { maxTimeMS => 5000 } );
        },
        undef,
        "distinct helper with maxTimeMS works"
    );

    is(
        exception {
            my $doc = $coll->find_one_and_replace(
                { _id    => 22 },
                { x      => 1 },
                { upsert => 1, maxTimeMS => 5000 }
            );
        },
        undef,
        "find_one_and_replace helper with maxTimeMS works"
    );

    is(
        exception {
            my $doc = $coll->find_one_and_update(
                { _id    => 23 },
                { '$set' => { x => 1 } },
                { upsert => 1, maxTimeMS => 5000 }
            );
        },
        undef,
        "find_one_and_update helper with maxTimeMS works"
    );

    is(
        exception {
            my $doc = $coll->find_one_and_delete( { _id => 23 }, { maxTimeMS => 5000 } );
        },
        undef,
        "find_one_and_delete helper with maxTimeMS works"
    );

    is(
        exception {
            my $cursor = $coll->database->list_collections( {}, { maxTimeMS => 5000 } );
        },
        undef,
        "list_collections command with maxTimeMS works"
    );

};

subtest "force maxTimeMS failures" => sub {
    skip_unless_min_version($conn, 'v2.6.0');

    # low batchSize to force multiple batches to get all docs
    my $cursor = $coll->find( {}, { batchSize => 5, maxTimeMS => 5000 } )->result;
    $cursor->next; # before turning on fail point

    is(
        exception {
            set_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' } );
        },
        undef,
        "turned on maxTimeAlwaysTimeOut fail point"
    );

    my @foo;
    like(
        exception { @foo = $cursor->all },
        qr/exceeded time limit/,
        "existing cursor with max_time_ms times out"
    ) or diag explain \@foo;

    like(
        exception { $coll->find()->max_time_ms(10)->next },
        qr/exceeded time limit/,
        "new cursor with max_time_ms times out"
    );

    like(
        exception { $coll->find( {}, { maxTimeMS => 10 } )->next },
        qr/exceeded time limit/,
        , "find with maxTimeMS times out"
    );

    like(
        exception {
            my $doc = $coll->find_one( { _id => 1 }, undef, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "find_one with maxTimeMS times out"
    );

    like(
        exception {
            my $doc = $coll->count_documents( {}, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "count command with maxTimeMS times out"
    );

    SKIP: {
        skip "aggregate not available until MongoDB v2.2", 1
            if check_min_server_version($conn, 'v2.2.0');

        like(
            exception {
                my $doc = $coll->aggregate(
                    [ { '$project' => { name => 1, count => 1 } } ],
                    { maxTimeMS => 10 },
                );
            },
            qr/exceeded time limit/,
            "aggregate helper with maxTimeMS times out"
        );
    }

    like(
        exception {
            my $doc = $coll->count_documents( {}, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "count helper with maxTimeMS times out"
    );

    like(
        exception {
            my $doc = $coll->distinct( 'a', {}, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "distinct helper with maxTimeMS times out"
    );

    like(
        exception {
            my $doc = $coll->find_one_and_replace(
                { _id    => 22 },
                { x      => 1 },
                { upsert => 1, maxTimeMS => 10 }
            );
        },
        qr/exceeded time limit/,
        "find_one_and_replace helper with maxTimeMS times out"
    );

    like(
        exception {
            my $doc = $coll->find_one_and_update(
                { _id    => 23 },
                { '$set' => { x => 1 } },
                { upsert => 1, maxTimeMS => 10 }
            );
        },
        qr/exceeded time limit/,
        "find_one_and_update helper with maxTimeMS times out"
    );

    like(
        exception {
            my $doc = $coll->find_one_and_delete( { _id => 23 }, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "find_one_and_delete helper with maxTimeMS times out"
    );

    like(
        exception {
            my $cursor = $coll->database->list_collections( {}, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "list_collections command times out"
    );

    subtest "max_time_ms via constructor" => sub {
        is(
            exception { my $doc = $coll->count_documents( {} ) },
            undef,
            "count helper with default maxTimeMS 0 from client works"
        );

        my $conn2   = build_client( max_time_ms => 10 );
        my $testdb2 = get_test_db($conn2);
        my $coll2   = $testdb2->get_collection("test_collection");

        like(
            exception {
                my $doc = $coll2->count_documents( {} );
            },
            qr/exceeded time limit/,
            "count helper with configured maxTimeMS times out"
        );
    };

    subtest "zero disables maxTimeMS" => sub {
        is( exception { $coll->find->max_time_ms(0)->next }, undef, "find->max_time_ms(0)" );
        is( exception { $coll->find( {}, { maxTimeMS => 5000 } ) },
            undef, "find with MaxTimeMS 5000 works" );

        is(
            exception {
                my $doc = $coll->find_one( { _id => 1 }, undef, { maxTimeMS => 0 } );
            },
            undef,
            "find_one with MaxTimeMS zero works"
        );

        SKIP: {
            skip "aggregate not available until MongoDB v2.2", 1
                if check_min_server_version($conn, 'v2.2.0');
            is(
                exception {
                    my $doc = $coll->aggregate(
                        [ { '$project' => { name => 1, count => 1 } } ],
                        { maxTimeMS => 0 },
                    );
                },
                undef,
                "aggregate helper with MaxTimeMS zero works"
            );
        }

        is(
            exception {
                my $doc = $coll->count_documents( {}, { maxTimeMS => 0 } );
            },
            undef,
            "count helper with MaxTimeMS zero works"
        );

        is(
            exception {
                my $doc = $coll->distinct( 'a', {}, { maxTimeMS => 0 } );
            },
            undef,
            "distinct helper with MaxTimeMS zero works"
        );

        is(
            exception {
                my $doc = $coll->find_one_and_replace(
                    { _id    => 22 },
                    { x      => 1 },
                    { upsert => 1, maxTimeMS => 0 }
                );
            },
            undef,
            "find_one_and_replace helper with MaxTimeMS zero works"
        );

        is(
            exception {
                my $doc = $coll->find_one_and_update(
                    { _id    => 23 },
                    { '$set' => { x => 1 } },
                    { upsert => 1, maxTimeMS => 0 }
                );
            },
            undef,
            "find_one_and_update helper with MaxTimeMS zero works"
        );

        is(
            exception {
                my $doc = $coll->find_one_and_delete( { _id => 23 }, { maxTimeMS => 0 } );
            },
            undef,
            "find_one_and_delete helper with MaxTimeMS zero works"
        );

    };

    is(
        exception {
            clear_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut' } );
        },
        undef,
        "turned off maxTimeAlwaysTimeOut fail point"
    );
};

subtest "create_many w/ maxTimeMS" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->drop;

    is(
        exception {
            set_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' } );
        },
        undef,
        'max time failpoint on',
    );

    like(
        exception {
            $iv->create_many(
                { keys => [ x => 1 ] }, { keys => [ y => -1 ] },
                { maxTimeMS => 10 },
            );
        },
        qr/exceeded time limit/,
        'timeout for index creation',
    );

    is(
        exception {
            $iv->create_many(
                { keys => [ x => 1 ] }, { keys => [ y => -1 ] },
            );
        },
        undef,
        'no timeout without max time',
    );

    is(
        exception {
            clear_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut' } );
        },
        undef,
        'max time failpoint off',
    );

    is(
        exception {
            $iv->create_many(
                { keys => [ x => 1 ] }, { keys => [ y => -1 ] },
                { maxTimeMS => 5000 },
            );
        },
        undef,
        'no timeout for index creation',
    );
};

subtest "create_one w/ maxTimeMS" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->drop;

    is(
        exception {
            set_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' } );
        },
        undef,
        'max time failpoint on',
    );

    is(
        exception {
            $iv->create_one([ x => 1 ]);
        },
        undef,
        'no timeout without max time',
    );

    like(
        exception {
            $iv->create_one([ x => 1 ], { maxTimeMS => 10 });
        },
        qr/exceeded time limit/,
        'timeout for index creation',
    );

    is(
        exception {
            clear_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut' } );
        },
        undef,
        'max time failpoint off',
    );

    is(
        exception {
            $iv->create_one([ x => 1 ], { maxTimeMS => 5000 });
        },
        undef,
        'no timeout for index creation',
    );
};

subtest "drop_one w/ maxTimeMS" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->drop;

    is(
        exception {
            set_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' } );
        },
        undef,
        'max time failpoint on',
    );

    is(
        exception {
            my $name = $iv->create_one([ x => 1 ]);
            $iv->drop_one($name);
        },
        undef,
        'no timeout without max time',
    );

    like(
        exception {
            my $name = $iv->create_one([ x => 1 ]);
            $iv->drop_one($name, { maxTimeMS => 10 });
        },
        qr/exceeded time limit/,
        'timeout for index drop',
    );

    is(
        exception {
          clear_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut' } );
        },
        undef,
        'max time failpoint off',
    );

    is(
        exception {
            my $name = $iv->create_one([ x => 1 ]);
            $iv->drop_one($name, { maxTimeMS => 5000 });
        },
        undef,
        'no timeout for index drop',
    );
};

subtest "drop_all w/ maxTimeMS" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    $coll->drop;

    is(
        exception {
            set_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' } );
        },
        undef,
        'max time failpoint on',
    );

    is(
        exception {
            $iv->create_many( map { { keys => $_ } }[ x => 1 ], [ y => 1 ], [ z => 1 ] );
            $iv->drop_all();
        },
        undef,
        'no timeout without max time',
    );

    like(
        exception {
            $iv->create_many( map { { keys => $_ } }[ x => 1 ], [ y => 1 ], [ z => 1 ] );
            $iv->drop_all({ maxTimeMS => 10 });
        },
        qr/exceeded time limit/,
        'timeout for index drop',
    );

    is(
        exception {
            clear_failpoint(
                $conn,
                { configureFailPoint => 'maxTimeAlwaysTimeOut' } );
        },
        undef,
        'max time failpoint off',
    );

    is(
        exception {
            $iv->create_many( map { { keys => $_ } }[ x => 1 ], [ y => 1 ], [ z => 1 ] );
            $iv->drop_all({ maxTimeMS => 5000 });
        },
        undef,
        'no timeout for index drop',
    );
};

done_testing;
