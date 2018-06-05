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
use Path::Tiny;

note("CAP-501 aggregation allowDiskUse");

my %config_map = (
    'mongod-2.6'  => 'host1',
    'sharded-2.6' => 'db1',
);

for my $deployment ( sort keys %config_map ) {
    subtest $deployment => sub {
        my $orc =
          MongoDBTest::Orchestrator->new( config_file => "devel/config/$deployment.yml" );
        $orc->start;
        $ENV{MONGOD} = $orc->as_uri;

        my $conn = build_client( dt_type => undef );
        my $testdb = get_test_db($conn);
        my $coll   = $testdb->get_collection("test_collection");

        $coll->insert_one( { count => $_ } ) for 1 .. 10;

        my $logfile =  $orc->get_server( $config_map{$deployment} )->logfile;

        my $res = $coll->aggregate( [ { '$project' => { _id => 1, count => 1 } } ] );

        my ($logline) = grep { /command: aggregate/i } $logfile->lines;

        unlike( $logline, qr/allowDiskUse/, "allowDiskUse not in log when not used" );

        $res = $coll->aggregate( [ { '$project' => { _id => 1, count => 1 } } ],
            { allowDiskUse => boolean::true} );

        (undef, $logline) = grep { /command: aggregate/i } $logfile->lines;

        like( $logline, qr/allowDiskUse/, "allowDiskUse in log when used" );

    };
}

clear_testdbs;

done_testing;
