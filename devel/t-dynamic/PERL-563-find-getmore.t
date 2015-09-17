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

sub _test_find_getmore {

    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );

    my $testdb = get_test_db($conn);
    my $server_version = server_version($conn);
    my $coll;
    my $cursor;

    $coll = $testdb->get_collection('test');
    $coll->drop;
    $coll->indexes->create_one({'sn'=>1});

    my $bulk = $coll->unordered_bulk;
    $bulk->insert_one({sn => $_}) for 0 .. 5000;
    $bulk->execute;

    $cursor = $coll->query;
    my $count = 0;
    while (my $doc = $cursor->next()) {
        $count++;
    }
    is(5001, $count);

    my @all = $coll->find->limit(3999)->all;
    is( 0+@all, 3999, "got limited documents" )
}

subtest "wire protocol 4" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-any.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_find_getmore();

    ok( scalar $orc->get_server('host1')->grep_log(qr/command: find/),
        "saw find in log" );
    ok( scalar $orc->get_server('host1')->grep_log(qr/command: getMore/),
        "saw getMore in log" );
};

subtest "wire protocol 3" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_find_getmore();
    
    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: find/),
        "no find in log" );
    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: getMore/),
        "no getMore in log" );
};

clear_testdbs;

done_testing;

