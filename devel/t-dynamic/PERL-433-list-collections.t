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
use MongoDBTest qw/build_client get_test_db clear_testdbs/;

sub _test_collection_names {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );
    my $testdb = get_test_db($conn);

    my $coll = $testdb->get_collection('test');

    my $cmd = [ create => "test_capped", capped => 1, size => 10000 ];
    $testdb->run_command($cmd);
    my $cap = $testdb->get_collection("test_capped");

    $coll->ensure_index( [ name => 1 ] );
    $cap->ensure_index( [ name => 1 ] );

    ok( $coll->insert( { name => 'Alice' } ), "create test collection" );
    ok( $cap->insert( { name => 'Bob' } ), "create capped collection" );

    my %names    = map {; $_ => 1 } $testdb->collection_names;
    for my $k ( qw/test test_capped/ ) {
       ok( exists $names{$k}, "saw $k in collection_names" );
    }
}

subtest "wire protocol 3" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-3.0.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_collection_names();

    ok( scalar $orc->get_server('host1')->grep_log(qr/command: listCollections/),
        "saw listCollections in log" );
};

subtest "wire protocol 0" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_collection_names();

    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: listCollections/),
        "no listCollections in log" );
};

clear_testdbs;

done_testing;

