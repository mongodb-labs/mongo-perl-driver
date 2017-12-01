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

my $index_spec = superhashof( { map { $_ => ignore() } qw/v key name ns/ } );

sub _test_index_names {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );
    my $testdb = get_test_db($conn);

    my $coll = $testdb->get_collection('test');

    is_deeply( [ $coll->indexes->list->all ], [], "no indexes yet" );

    ok( $coll->indexes->create_one( [ name => 1 ] ), "single-field index" );
    ok( $coll->indexes->create_one( [ name => 1, age => 1 ] ), "compound index");
    ok( $coll->indexes->create_one( [ ssn => 1 ], {unique => 1} ), "unique index");

    ok( $coll->insert_one( { name => 'Alice', age => 23, ssn => "999-88-7777" } ), "insert doc" );

    my @indexes = $coll->indexes->list->all;
    is( scalar @indexes, 4, "right number of indexes" );
    cmp_deeply( $_, $index_spec, "$_->{name} index looks right" ) for @indexes;
}

subtest "wire protocol 3" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-3.0.yml" );
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_index_names();

    ok( scalar $orc->get_server('host1')->grep_log(qr/command: listIndexes/),
        "saw listIndexes in log" );
};

subtest "wire protocol 0" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_index_names();

    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: listIndexes/),
        "no listIndexes in log" );
};

clear_testdbs;

done_testing;

