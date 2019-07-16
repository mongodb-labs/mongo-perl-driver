#  Copyright 2017 - present MongoDB, Inc.
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
use MongoDB::Error;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    get_unique_collection
    skip_unless_min_version
/;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);

plan skip_all => "Profiler doesn't work on mongos"
    if $server_type eq 'Mongos';

my $supports_collation = $server_version >= 3.3.9;
my $case_insensitive_collation = { locale => "en_US", strength => 2 };

subtest "aggregation comment" => sub {
    skip_unless_min_version($conn, 'v3.6.0');

    my $coll = get_unique_collection( $testdb, "agg_comm" );

    $coll->insert_many( [ { _id => 1, category => "cake", type => "chocolate", qty => 10 },
                          { _id => 2, category => "cake", type => "ice cream", qty => 25 },
                          { _id => 3, category => "pie", type => "boston cream", qty => 20 },
                          { _id => 4, category => "pie", type => "blueberry", qty => 15 } ] );

    #turn on profiling if not already enabled
    my $previous_profile_setting = $testdb->run_command( { profile => -1 } )->{was};
    $testdb->run_command( { profile => 2 } );

    my $profile_coll = $testdb->get_collection('system.profile');

    my $cursor_no_comment = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } }
        ],
    );

    my $cursor_with_comment = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } }
        ],
        { comment => "This is only a test" }
    );

    my $result_no_comment = $cursor_no_comment->next;
    my $result_with_comment = $cursor_with_comment->next;

    is( ref( $result_no_comment ), 'HASH', "aggregate returned a result" );
    is( ref( $result_with_comment ), 'HASH', "aggregate returned a result" );

    # pull out profiling parts for the aggregates above in time order
    my @all_profiles = $profile_coll->find({ 'command.aggregate' => $coll->name })->sort([ ts => 1 ])->all;

    is( $all_profiles[-2]->{command}->{aggregate}, $coll->name, "Found aggregate command" );
    ok( ! exists $all_profiles[-2]->{command}->{comment}, "No comment on first aggregate" );

    is( $all_profiles[-1]->{command}->{aggregate}, $coll->name, "Found second aggregate command" );
    is( $all_profiles[-1]->{command}->{comment}, "This is only a test", "Found comment on second aggregate" );

    $testdb->run_command( { profile => $previous_profile_setting } );

    $coll->drop;
};

done_testing;
