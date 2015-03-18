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
use Test::More;
use Config;
BEGIN { plan skip_all => 'requires threads' unless $Config{usethreads} }

use MongoDB;
use Try::Tiny;
use threads;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $testdb = get_test_db(build_client());

my $col = $testdb->get_collection('tiger');
$col->drop;


$col->insert_one({ foo => 9,  bar => 3, shazbot => 1 });
$col->insert_one({ foo => 2,  bar => 5 });
$col->insert_one({ foo => -3, bar => 4 });
$col->insert_one({ foo => 4,  bar => 9, shazbot => 1 });


{
    my $cursor = $col->query;

    # force start of retrieval before creating threads
    $cursor->next;

    my $ret = threads->create(sub {
        $cursor->next;
    })->join;

    is_deeply $ret, $cursor->next,
        'cursors retain their position on thread cloning';
}

{
    my $cursor = threads->create(sub {
        my $cursor = $col->query;

        # force start of retrieval before returning the cursor
        $cursor->next;

        return $cursor;
    })->join;

    # cursor for comparison
    my $comp_cursor = $col->query;

    # seek as far ahead as we did within the thread
    $comp_cursor->next;

    is_deeply $cursor->next, $comp_cursor->next,
        'joining back cursors works';
}

{
    my $cursor = $col->query;

    # force start of retrieval before creating threads
    $cursor->next;

    my @threads = map {
        threads->create(sub {
            $cursor->next;
        });
    } 0 .. 9;

    my @ret = map { $_->join } @threads;

    is_deeply [@ret], [($cursor->next) x 10],
        'cursors retain their position on thread cloning';
}

{
    my @threads = map {
        threads->create(sub {
            my $cursor = $col->query;

            # force start of retrieval before returning the cursor
            $cursor->next;

            return $cursor;
        })
    } 0 .. 9;

    my @cursors = map { $_->join } @threads;

    # cursor for comparison
    my $comp_cursor = $col->query;

    # seek as far ahead as we did within the thread
    $comp_cursor->next;

    is_deeply [map { $_->next } @cursors], [($comp_cursor->next) x 10],
        'joining back cursors works';
}

done_testing();
