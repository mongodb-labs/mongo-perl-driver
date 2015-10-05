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

sub _test_read_concern_set {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );

    my $server_version = server_version($conn);

    my $coll = $conn->get_database('test_db')->get_collection('test', {read_concern => {level => 'majority'}});

    $coll->drop;
    $coll->insert_one({ a => 1 });
    is( $coll->count, 1, "one doc in collection");
    my $cursor = $coll->find( {} );
    my $obj = $cursor->next;

    is($obj->{a}, 1, 'dummy test');
}

sub _test_read_concern_not_set {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );

    my $server_version = server_version($conn);

    my $coll = $conn->get_database('test_db')->get_collection('test');

    $coll->drop;
    $coll->insert_one({ a => 1 });
    is( $coll->count, 1, "one doc in collection");
    my $cursor = $coll->find( {} );
    my $obj = $cursor->next;

    is($obj->{a}, 1, 'dummy test');
}

subtest "wire protocol 4" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-any.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_read_concern_set();
    ok( scalar $orc->get_server('host1')->grep_log(qr/readConcern/),
        "saw readConcern in log" );
};

subtest "wire protocol 4 : readConcern not set" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-any.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_read_concern_not_set();

    ok( !scalar $orc->get_server('host1')->grep_log(qr/readConcern/),
        "no readConcern in log" );
};

subtest "wire protocol 3" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_read_concern_set();

    ok( !scalar $orc->get_server('host1')->grep_log(qr/readConcern/),
        "no readConcern in log" );
};

clear_testdbs;

done_testing;

