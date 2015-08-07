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
use Test::More;
use Test::Fatal;

use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $conn   = build_client();
my $testdb = get_test_db($conn);

ok( $conn->connected, "client is connected" );
isa_ok( $conn, 'MongoDB::MongoClient' );

subtest "bad seedlist" => sub {
    my $conn2;

    is(
        exception {
            $conn2 = build_client(
                host                     => 'localhost',
                port                     => 1,
                connect_timeout_ms       => 1000,
                server_selection_timeout => 1,
            );
        },
        undef,
        'no exception on construction for bad port'
    );

    ok( !$conn2->connected, "bad port reports not connected" );
};

subtest "get_database and check names" => sub {
    my $db = $conn->get_database( $testdb->name );
    isa_ok( $db, 'MongoDB::Database', 'get_database' );

    $db->get_collection('test_collection')->insert_one( { foo => 42 } );

    ok( ( grep { /testdb/ } $conn->database_names ), 'database_names' );

    my $result = $db->drop;
    is( $result->{'ok'}, 1, 'db was dropped' );
};

subtest "wire protocol versions" => sub {
    is $conn->MIN_WIRE_VERSION, 0, 'default min wire version';
    is $conn->MAX_WIRE_VERSION, 3, 'default max wire version';

    # monkey patch wire versions
    my $conn2 = build_client();
    $conn2->_topology->{min_wire_version} = 100;
    $conn2->_topology->{max_wire_version} = 101;

    like(
        exception { $conn2->send_admin_command( [ is_master => 1 ] ) },
        qr/Incompatible wire protocol/i,
        'exception on wire protocol'
    );

};

subtest "reconnect" => sub {
    ok( $testdb->_client->reconnect, "ran reconnect" );
    my $db = $conn->get_database( $testdb->name );
    ok( $db->get_collection('test_collection')->insert_one( { foo => 42 } ),
        "inserted a doc after reconnection"
    );
};

subtest "topology status" => sub {
    my $res = $conn->topology_status( );
    is( ref($res), 'HASH', "topology_status returns a hash reference" );
    my $last = $res->{last_scan_time};
    sleep 1;
    $res = $conn->topology_status( refresh => 1 );
    ok( $res->{last_scan_time} > $last, "scan time refreshed" );
};

subtest "cooldown" => sub {
    my $conn = build_client( host => "mongodb://localhost:9" );
    my $topo = $conn->_topology;
    $topo->scan_all_servers;
    my $orig_update = $topo->status_struct->{servers}[0]{last_update_time};
    $topo->scan_all_servers;
    my $next_update = $topo->status_struct->{servers}[0]{last_update_time};
    is( $next_update, $orig_update, "Unknown server not scanned again during cooldown" );
};

done_testing;
