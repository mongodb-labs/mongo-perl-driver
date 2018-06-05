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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs/;

note("CAP-420 Mixed mode sharded cluster testing");

subtest "2.6 Mongos + 2.4, 2.6 shards" => sub {
    my $orc =
    MongoDBTest::Orchestrator->new(
        config_file => "devel/t-dynamic/sharded-2.6-mixed.yml" );
    $orc->start;
    $ENV{MONGOD} = $orc->as_uri;

    my $conn = build_client( dt_type => undef );
    my $testdb = get_test_db($conn);
    my $coll   = $testdb->get_collection("test_collection");

    $coll->insert_many( [ { wanted => 1, score => 56 },
                           { wanted => 1, score => 72 },
                           { wanted => 1, score => 96 },
                           { wanted => 1, score => 32 },
                           { wanted => 1, score => 61 },
                           { wanted => 1, score => 33 },
                           { wanted => 0, score => 1000 } ] );

    pass( "data inserted" );
    # put DB on shard1, which is the 2.4 one, which should fail with cursor
    eval {
        my $admin = $conn->get_database("admin");
        $admin->run_command([movePrimary => $testdb->name, to => 'sh1']);
        $admin->run_command([flushRouterConfig => 1]);
    };
    pass( "migrated data to alternate shard" );

    like(
        exception { $coll->aggregate( [ {'$match' => { count => {'$gt' => 0} } } ] ) },
        qr/unrecognized field.*cursor/,
        "2.6+2.4: default aggregation fails"
    );

    my $res = $coll->aggregate(
        [
            { '$match' => { wanted => 1 } },
            { '$group' => { _id    => 1, 'avgScore' => { '$avg' => '$score' } } }
        ],
        { cursor => undef }
    );

    is( ref( $res ), 'MongoDB::QueryResult',  "2.6+2.4: manually disabling cursor works" );
    $res = $res->next;
    ok $res->{avgScore} < 59;
    ok $res->{avgScore} > 57;

    # put DB on shard2, which is the 2.6 one, which should succeed
    eval {
        my $admin = $conn->get_database("admin");
        $admin->run_command([movePrimary => $testdb->name, to => 'sh2']);
        $admin->run_command([flushRouterConfig => 1]);
    };

    $res = $coll->aggregate( [ { '$match'   => { wanted => 1 } },
                                  { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } } ] );

    is( ref( $res ), 'MongoDB::QueryResult',  "2.6+2.6: default aggregation works" );
    $res = $res->next;
    ok $res->{avgScore} < 59;
    ok $res->{avgScore} > 57;

    is(
        exception { $coll->aggregate( [ {'$match' => { count => {'$gt' => 0} } } ], { cursor => undef } ) },
        undef,
        "2.6+2.6: disabling cursor when supported is also fine"
    );

};

subtest "2.4 Mongos + 2.4, 2.6 shards" => sub {
    my $orc =
    MongoDBTest::Orchestrator->new(
        config_file => "devel/t-dynamic/sharded-2.4-mixed.yml" );
    $orc->start;
    $ENV{MONGOD} = $orc->as_uri;

    my $conn = build_client( dt_type => undef );

    my $testdb = get_test_db($conn)   ;
    my $coll   = $testdb->get_collection("test_collection");

    $coll->insert_many( [ { wanted => 1, score => 56 },
                           { wanted => 1, score => 72 },
                           { wanted => 1, score => 96 },
                           { wanted => 1, score => 32 },
                           { wanted => 1, score => 61 },
                           { wanted => 1, score => 33 },
                           { wanted => 0, score => 1000 } ] );

    # put DB on shard1, which is the 2.4 one, which should fail with cursor
    eval {
        my $admin = $conn->get_database("admin");
        $admin->run_command([movePrimary => $testdb->name, to => 'sh1']);
        $admin->run_command([flushRouterConfig => 1]);
    };

    my $res = $coll->aggregate( [ { '$match'   => { wanted => 1 } },
                                  { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } } ] );

    is( ref( $res ), 'MongoDB::QueryResult',  "2.4+2.4: default aggregation works" );
    $res = $res->next;
    ok $res->{avgScore} < 59;
    ok $res->{avgScore} > 57;

    is(
        exception { $coll->aggregate( [ {'$match' => { count => {'$gt' => 0} } } ], { cursor => {} } ) },
        undef,
        "2.4+2.4: asking for cursor when unsupported is ignored"
    );

    # put DB on shard2, which is the 2.6 one, which should still fail 
    eval {
        my $admin = $conn->get_database("admin");
        $admin->run_command([movePrimary => $testdb->name, to => 'sh2']);
        $admin->run_command([flushRouterConfig => 1]);
    };

    $res = $coll->aggregate( [ { '$match'   => { wanted => 1 } },
                                  { '$group'   => { _id => 1, 'avgScore' => { '$avg' => '$score' } } } ] );

    is( ref( $res ), 'MongoDB::QueryResult',  "2.4+2.6: default aggregation works" );
    $res = $res->next;
    ok $res->{avgScore} < 59;
    ok $res->{avgScore} > 57;

    is(
        exception { $coll->aggregate( [ {'$match' => { count => {'$gt' => 0} } } ], { cursor => {} } ) },
        undef,
        "2.4+2.6: asking for cursor when unsupported is ignored"
    );

};

clear_testdbs;

done_testing;
