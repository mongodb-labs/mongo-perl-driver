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
use MongoDBTest qw/build_client get_test_db server_type server_version/;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_type = server_type($conn);
my $server_version = server_version($conn);

plan skip_all => "maxTimeMS not available before 2.6"
  unless $server_version >= v2.6.0;

my $param = eval {
    $conn->get_database('admin')
      ->_try_run_command( [ getParameter => 1, enableTestCommands => 1 ] );
};

my $coll;
my $admin = $conn->get_database("admin");

note "CAP-401 test plan";

can_ok( 'MongoDB::Cursor', 'max_time_ms' );

$coll = $testdb->get_collection("test_collection");

my $bulk = $coll->ordered_bulk;
$bulk->insert( { _id => $_ } ) for 1 .. 20;
my $err = exception { $bulk->execute };
is( $err, undef, "inserted 20 documents for testing" );

subtest "expected behaviors" => sub {

    is( exception { $coll->find->max_time_ms()->next },  undef, "find->max_time_ms()" );
    is( exception { $coll->find->max_time_ms(0)->next }, undef, "find->max_time_ms(0)" );
    is( exception { $coll->find->max_time_ms(5000)->next },
        undef, "find->max_time_ms(5000)" );

    like( exception { $coll->find->max_time_ms(-1)->next },
        qr/non-negative/, "find->max_time_ms(-1) throws exception" );

    is(
        exception {
            my $doc = $coll->find_one( { '$query' => { _id => 1 }, '$maxTimeMS' => 5000 } );
        },
        undef,
        "find_one with \$query and \$maxTimeMS works"
    );

    is(
        exception {
            my $doc = $testdb->_try_run_command( [ count => $coll->name, maxTimeMS => 5000 ] );
        },
        undef,
        "count command with maxTimeMS works"
    );

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

};

subtest "force maxTimeMS failures" => sub {
    plan skip_all => "enableTestCommands is off"
      unless $param && $param->{enableTestCommands};

    plan skip_all => "fail points not supported via mongos"
      if $server_type eq 'Mongos';

    my $cursor = $coll->find( {} )->max_time_ms(5000);
    $cursor->_batch_size(5); # force multiple batches to get all docs
    $cursor->next;           # before turning on fail point

    is(
        exception {
            $admin->_try_run_command(
                [ configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'alwaysOn' ] );
        },
        undef,
        "turned on maxTimeAlwaysTimeOut fail point"
    );

    my @foo;
    like(
        exception { @foo = $cursor->all },
        qr/exceeded time limit/,
        "existing cursor with max_time_ms times out getting results"
    ) or diag explain \@foo;

    like(
        exception { $coll->find()->max_time_ms(10)->next },
        qr/exceeded time limit/,
        "new cursor with max_time_ms times out getting results"
    );

    like(
        exception {
            my $doc = $testdb->_try_run_command( [ count => $coll->name, maxTimeMS => 10 ] );
        },
        qr/exceeded time limit/,
        "count command with maxTimeMS times out getting results"
    );

    like(
        exception {
            my $doc = $coll->aggregate(
                [ { '$project' => { name => 1, count => 1 } } ],
                { maxTimeMS => 10 },
            );
        },
        qr/exceeded time limit/,
        "aggregate helper with maxTimeMS times out getting results"
    );

    is(
        exception {
            $admin->_try_run_command(
                [ configureFailPoint => 'maxTimeAlwaysTimeOut', mode => 'off' ] );
        },
        undef,
        "turned off maxTimeAlwaysTimeOut fail point"
    );
};

done_testing;
