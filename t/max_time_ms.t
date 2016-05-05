#
#  Copyright 2014 MongoDB, Inc.
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

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_type server_version/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_type    = server_type($conn);
my $server_version = server_version($conn);

# This test sets failpoints, which will make the tested server unusable
# for ordinary purposes. As this is risky, the test requires the user
# to opt-in
unless ( $ENV{FAILPOINT_TESTING} ) {
    plan skip_all => "\$ENV{FAILPOINT_TESTING} is false";
}

# Test::Harness 3.31 supports the t/testrules.yml file to ensure that
# this test file won't be run in parallel other tests, since turning on
# a fail point will interfere with other tests.
if ( $ENV{HARNESS_VERSION} < 3.31 ) {
    plan skip_all => "not safe to run fail points before Test::Harness 3.31";
}

my $param = eval {
    $conn->get_database('admin')
      ->run_command( [ getParameter => 1, enableTestCommands => 1 ] );
};

my $coll;
my $admin = $conn->get_database("admin");

note "CAP-401 test plan";

can_ok( 'MongoDB::Cursor', 'max_time_ms' );

$coll = $testdb->get_collection("test_collection");

my $bulk = $coll->ordered_bulk;
$bulk->insert_one( { _id => $_ } ) for 1 .. 20;
my $err = exception { $bulk->execute };
is( $err, undef, "inserted 20 documents for testing" );

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
            unless $server_version > v2.2.0;

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
            my $doc = $coll->count( {}, { maxTimeMS => 5000 } );
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

    subtest "parallel_scan" => sub { 
        plan skip_all => "Parallel scan not supported before MongoDB 2.6"
        unless $server_version >= v2.6.0;
        plan skip_all => "Parallel scan not supported on mongos"
        if $server_type eq 'Mongos';

        is(
            exception {
                my $cursor = $coll->parallel_scan( 20, { maxTimeMS => 5000 } );
            },
            undef,
            "parallel_scan command with maxTimeMS works"
        );
    };

};

subtest "force maxTimeMS failures" => sub {
    plan skip_all => "maxTimeMS not available before 2.6"
      unless $server_version >= v2.6.0;

    plan skip_all => "enableTestCommands is off"
      unless $param && $param->{enableTestCommands};

    plan skip_all => "fail points not supported via mongos"
      if $server_type eq 'Mongos';

    # low batchSize to force multiple batches to get all docs
    my $cursor = $coll->find( {}, { batchSize => 5, maxTimeMS => 5000 } )->result;
    $cursor->next; # before turning on fail point

    is(
        exception {
            $admin->run_command(
                [ configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' ] );
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
            my $doc = $coll->count( {}, { maxTimeMS => 10 } );
        },
        qr/exceeded time limit/,
        "count command with maxTimeMS times out"
    );

    SKIP: {
        skip "aggregate not available until MongoDB v2.2", 1
            unless $server_version > v2.2.0;

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
            my $doc = $coll->count( {}, { maxTimeMS => 10 } );
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

    subtest "parallel_scan" => sub { 
        plan skip_all => "Parallel scan not supported before MongoDB 2.6"
        unless $server_version >= v2.6.0;
        plan skip_all => "Parallel scan not supported on mongos"
        if $server_type eq 'Mongos';

        like(
            exception {
                my $cursor = $coll->parallel_scan( 20, { maxTimeMS => 10 } );
            },
            qr/exceeded time limit/,
            "parallel_scan command times out"
        );
    };

    subtest "max_time_ms via constructor" => sub {
        is(
            exception { my $doc = $coll->count( {} ) },
            undef,
            "count helper with default maxTimeMS 0 from client works"
        );

        my $conn2   = build_client( max_time_ms => 10 );
        my $testdb2 = get_test_db($conn2);
        my $coll2   = $testdb2->get_collection("test_collection");

        like(
            exception {
                my $doc = $coll2->count( {} );
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
                unless $server_version > v2.2.0;
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
                my $doc = $coll->count( {}, { maxTimeMS => 0 } );
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
            $admin->run_command(
                [ configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'off' ] );
        },
        undef,
        "turned off maxTimeAlwaysTimeOut fail point"
    );
};

done_testing;
