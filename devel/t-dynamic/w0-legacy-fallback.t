#
#  Copyright 2015 MongoDB, Inc.
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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs server_version/;

subtest "w0 doesn't send write commands" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-any.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    my $conn   = build_client( dt_type => undef );
    my $testdb = get_test_db($conn);
    my $coll   = $testdb->get_collection( 'test', { write_concern => { w => 0 } } );
    my $res;

    $res = $coll->insert_one( { a => 1 } );
    isa_ok( $res, "MongoDB::UnacknowledgedResult", "insert_one" );
    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: insert/), "no insert command in log" );
    is( $coll->count, 1, "inserted one doc" );

    $res = $coll->update_one( { a => 1 }, { '$inc' => { a => 2 } } );
    isa_ok( $res, "MongoDB::UnacknowledgedResult", "update_one" );
    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: update/), "no update command in log" );

    $res = $coll->delete_one( { a => 3 } );
    isa_ok( $res, "MongoDB::UnacknowledgedResult", "delete_one" );
    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: delete/), "no delete command in log" );
    is( $coll->count, 0, "no docs left" );

    my @ids = $coll->batch_insert( [ map { { a => $_ } } 2 .. 10 ] );
    is( scalar @ids, 0, "batch_insert returns no ids" );
    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: insert/), "no insert command in log" );
    is( $coll->count, 9, "nine docs left" );

    ok( !scalar $orc->get_server('host1')->grep_log(qr/getLastError/),
        "no GLE in log" );

};

clear_testdbs;

done_testing;

