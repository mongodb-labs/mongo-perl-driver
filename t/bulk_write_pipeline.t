#  Copyright 2019 - present MongoDB, Inc.
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
    skip_unless_min_version
/;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);

skip_unless_min_version($conn, 'v4.1.11');

my @tests = (
    {
        name     => 'BulkWrite Update One Pipeline',
        input    => [
            {
                "_id" => 1,
                "x"   => 1,
                "y"   => 1,
                "t"   => { "u" => { "v" => 1 } }
            },
            {
                "_id" => 2,
                "x"   => 2,
                "y"   => 1
            },
        ],
        method   => 'update_one',
        find     => { _id => 1 },
        pipeline => bson_array(
            { '$replaceRoot' => { "newRoot" => '$t' } },
            { '$addFields' => { "foo" => 1 } },
        ),
        output   => [
            {
                _id => 1,
                foo => 1,
                u   => {
                    v => 1,
                },
            },
            {
                _id => 2,
                x   => 2,
                y   => 1,
            },
        ]
    },
    {
        name => 'BulkWrite Update Many Pipeline',
        input    => [
            {
                "_id" => 1,
                "x"   => 1,
                "y"   => 1,
                "t"   => { "u" => { "v" => 1 } }
            },
            {
                "_id" => 2,
                "x"   => 1,
                "y"   => 1,
                "t"   => { "u" => { "v" => 1 } }
            },
            {
                "_id" => 3,
                "x"   => 1,
                "y"   => 1,
                "t"   => { "u" => { "v" => 1 } }
            },
            {
                "_id" => 4,
                "x"   => 2,
                "y"   => 1
            },
        ],
        method   => 'update_many',
        find     => { x => 1 },
        pipeline => bson_array(
            { '$replaceRoot' => { "newRoot" => '$t' } },
            { '$addFields' => { "foo" => 1 } },
        ),
        output   => [
            {
                _id => 1,
                foo => 1,
                u   => {
                    v => 1,
                },
            },
            {
                _id => 2,
                foo => 1,
                u   => {
                    v => 1,
                },
            },
            {
                _id => 3,
                foo => 1,
                u   => {
                    v => 1,
                },
            },
            {
                _id => 4,
                x   => 2,
                y   => 1,
            },
        ]
    }
);

for my $test ( @tests ) {
    my $coll = $testdb->get_collection('test_collection');
    subtest $test->{name} => sub {
        $coll->insert_many($test->{input});
        my $bulk = $coll->ordered_bulk;
        my $method = $test->{method};
        $bulk->find($test->{find})->$method($test->{pipeline});
        $bulk->execute;

        my @output = $coll->find({})->all;

        is_deeply( \@output, $test->{output}, 'Output as expected' );
    };
    $coll->drop;
}

done_testing;