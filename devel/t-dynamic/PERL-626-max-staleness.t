#
#  Copyright 2016 MongoDB, Inc.
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
use Time::HiRes qw/time/;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs server_version get_capped/;

for my $config ( qw/sharded-any.yml sharded-2mongos.yml/ ) {

    subtest "max_staleness_ms passthrough to $config" => sub {
        my $orc =
        MongoDBTest::Orchestrator->new(
            config_file => "devel/config/$config",
            log_verbose => 1,
            verbose => $ENV{MONGOVERBOSE} ? 1 : 0,
        );
        note "Starting deployment";
        $orc->start;
        local $ENV{MONGOD} = $orc->as_uri;

        my $conn = build_client( dt_type => undef, host => "$ENV{MONGOD}/?readPreference=secondary&maxStalenessMS=120000" );

        my $testdb = get_test_db($conn );
        my $server_version = server_version($conn);
        note "Server Version: $server_version";

        my $coll = $testdb->coll("test");
        $coll->drop;
        $coll->insert_one({x => 1});

        my $res = $coll->find_one( {} );

        is( $res->{x}, 1, "find_one worked" );

        my $saw_max_stale = grep { /\$readPreference.*maxStalenessMS.*120000/ }
          map { $_->logfile->lines } $orc->deployment->routers->all_servers;

        ok( $saw_max_stale, "maxStalenessMS applied to MongoS" )
          or diag $orc->get_server('router1')->grep_log(qr/\$readPreference/)
          and diag $orc->get_server('router2')->grep_log(qr/\$readPreference/);

        note "Shuttind down deployment";
    };

}

clear_testdbs;

done_testing;

