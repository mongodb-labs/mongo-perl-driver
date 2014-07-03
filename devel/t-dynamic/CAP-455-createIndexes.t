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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use MongoDBTest::Orchestrator;
use MongoDBTest;
use Path::Tiny;

note("CAP-455 createIndexes");

my %config_map = (
    'mongod-2.6'  => 'host1',
    'sharded-2.6' => 'db1',
);

for my $cluster ( sort keys %config_map ) {
    subtest $cluster => sub {
        my $orc =
          MongoDBTest::Orchestrator->new( config_file => "devel/clusters/$cluster.yml" );
        diag "starting cluster";
        $orc->start;
        $ENV{MONGOD} = $orc->as_uri;
        diag "MONGOD: $ENV{MONGOD}";

        my $conn   = MongoDBTest::build_client( dt_type => undef );
        my $testdb = $conn->get_database( MongoDBTest::rand_db_name() );
        my $coll   = $testdb->get_collection("test_collection");

        $coll->insert( { count => $_ } ) for 1 .. 10;

        my $logfile =  $orc->get_server( $config_map{$cluster} )->logfile;

        my $res = $coll->ensure_index( [ count => 1 ] );

        ok( (grep { /command: createIndexes/i } $logfile->lines ), "createIndexes found in log" );

        ok( (! grep { /insert.*system\.indexes/i } $logfile->lines ), "insert to system.indexes not found in log" );

    };
}

done_testing;
