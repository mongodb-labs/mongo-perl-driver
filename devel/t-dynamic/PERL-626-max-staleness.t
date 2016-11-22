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
use Time::HiRes qw/time usleep/;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs server_version get_capped/;

subtest "Replica Set with wire version >= 5" => sub {
    my $config = "replicaset-any.yml";
    my $orc    = MongoDBTest::Orchestrator->new(
        config_file => "devel/config/$config",
        log_verbose => 1,
        verbose     => $ENV{MONGOVERBOSE} ? 1 : 0,
    );
    note "Starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    my $uri = $ENV{MONGOD};
    $uri =~ s/\?/?heartbeatFrequencyMS=500&/;
    my $conn = build_client( dt_type => undef, host => $uri );

    is( $conn->heartbeat_frequency_ms, 500, "heartbeatFrequencyMS set to 500" );

    my $testdb         = get_test_db($conn);
    my $server_version = server_version($conn);
    my $coll = $testdb->coll("test");

    note "Server Version: $server_version";

    subtest "Verify heartbeatFrequencyMS is configurable" => sub {
        $coll->drop;

        # Do some work for several seconds to keep up communications to the
        # database, which should trigger isMaster calls.
        my $now = time;
        my $secs = 4;
        while ( time - $now < $secs ) {
            $coll->insert_one( { deadbeef => 1 } );
            usleep( 100000 );
        }

        # Extract log for a server.
        my ($a_server) =$orc->deployment->all_servers;
        my @log_lines = $a_server->logfile->lines;

        # Hunt log lines for first "deadbeef" insert.
        my $i = 0;
        while ( $log_lines[$i] !~ /deadbeef/ ) { $i++ }
        my $first = $i;

        # Hunt for last deadbeef insert.
        $i = $#log_lines;
        while ( $log_lines[$i] !~ /deadbeef/ ) { $i-- }
        my $last = $i;

        # Count number of isMaster calls between those points.  Over 4 seconds,
        # we should see more than 1 per second and less than 3 per second, even
        # accounting for some server lag.
        my $count = grep { /isMaster/ } @log_lines[ $first .. $last ];
        my $max = 3 * $secs;
        my $min = $secs;
        ok( $count > $min && $count < $max, "Got expected range of isMaster calls" )
            or diag "Got $count, wanted $min < x < $max; log range was " . ($last - $first) . " lines.";
    };

    subtest "Verify parse last write date" => sub {
        my $primary;

        ok( $coll->insert_one( {} ), "Inserted a document" );
        usleep 1e6;

        $conn->_topology->scan_all_servers;
        ($primary) = grep { $_->type eq 'RSPrimary' } $conn->_topology->all_servers;
        my $orig_write_date = $primary->last_write_date();
        ok( $orig_write_date, "Server has lastWriteDate" );

        ok( $coll->insert_one( {} ), "Inserted a document" );
        usleep 1e6;

        $conn->_topology->scan_all_servers;
        ($primary) = grep { $_->type eq 'RSPrimary' } $conn->_topology->all_servers;
        my $new_write_date = $primary->last_write_date();
        ok( $new_write_date > $orig_write_date && $new_write_date < $orig_write_date + 10,
            "lastWriteDate in expected range" )
          or diag "Old: $orig_write_date, New: $new_write_date\n";
    };

    note "Shutting down deployment";
};

subtest "Replica Set with wire version < 5" => sub {
    my $config = "replicaset-3.2.yml";
    my $orc    = MongoDBTest::Orchestrator->new(
        config_file => "devel/config/$config",
        log_verbose => 1,
        verbose     => $ENV{MONGOVERBOSE} ? 1 : 0,
    );
    note "Starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    my $uri = $ENV{MONGOD};
    $uri =~ s/\?/?heartbeatFrequencyMS=500&/;
    my $conn = build_client( dt_type => undef, host => $uri );

    is( $conn->heartbeat_frequency_ms, 500, "heartbeatFrequencyMS set to 500" );

    my $testdb         = get_test_db($conn);
    my $server_version = server_version($conn);
    my $coll = $testdb->coll("test");

    note "Server Version: $server_version";

    subtest "Verify no last write date" => sub {
        ok( $coll->insert_one( {} ), "Inserted a document" );
        my @servers         = $conn->_topology->all_servers;
        my $orig_write_date = $servers[0]->last_write_date();
        is( $orig_write_date, 0, "Server has no lastWriteDate" );
    };

    note "Shutting down deployment";
};

my @sharded_cases = (
    ["Mongos with Standalone Topology", 'sharded-any.yml'],
    ["Mongos with Sharded Topology", 'sharded-2mongos.yml'],
);

for my $case( @sharded_cases ) {
    my ($label, $config) = @$case;
    subtest "$label: max_staleness_seconds passthrough" => sub {
        my $orc =
        MongoDBTest::Orchestrator->new(
            config_file => "devel/config/$config",
            log_verbose => 1,
            verbose => $ENV{MONGOVERBOSE} ? 1 : 0,
        );
        note "Starting deployment";
        $orc->start;
        local $ENV{MONGOD} = $orc->as_uri;

        my $conn = build_client( dt_type => undef, host => "$ENV{MONGOD}/?readPreference=secondary&maxStalenessSeconds=120" );

        my $testdb = get_test_db($conn );
        my $server_version = server_version($conn);
        note "Server Version: $server_version";

        my $coll = $testdb->coll("test");
        $coll->drop;
        $coll->insert_one({x => 1});

        my $res = $coll->find_one( {} );

        is( $res->{x}, 1, "find_one worked" );

        my $saw_max_stale = grep { /\$readPreference.*maxStalenessSeconds.*120/ }
          map { $_->logfile->lines } $orc->deployment->routers->all_servers;

        ok( $saw_max_stale, "maxStalenessSeconds applied to MongoS" )
          or diag $orc->get_server('router1')->grep_log(qr/\$readPreference/)
          and diag $orc->get_server('router2')->grep_log(qr/\$readPreference/);

        note "Shutting down deployment";
    };

}

clear_testdbs();

done_testing;

