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
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client();
my $testdb = get_test_db($conn);

ok( $conn->connected, "client is connected" );
ok( !build_client( host => "mongodb://localhost:9" )->connected,
    "bogus client not connected" );

is(
    exception { MongoDB::MongoClient->new(host => 'localhost', port => 1, ssl => $ENV{MONGO_SSL}); },
    undef,
    'no exception on connection failure during topology scan'
);

SKIP: {
    skip "connecting to default host/port won't work with a remote db", 13 if exists $ENV{MONGOD};

    is(
        exception { $conn = MongoDB::MongoClient->new(ssl => $ENV{MONGO_SSL}); },
        undef,
        'successful connection'
    ) ;

    isa_ok($conn, 'MongoDB::MongoClient');

    is($conn->host, 'mongodb://localhost:27017', 'host default value');
    is($conn->db_name, '', 'db_name default value');

    # just make sure a couple timeouts work
    my $to = MongoDB::MongoClient->new('timeout' => 1, ssl => $ENV{MONGO_SSL});
    $to = MongoDB::MongoClient->new('timeout' => 123, ssl => $ENV{MONGO_SSL});
    $to = MongoDB::MongoClient->new('timeout' => 2000000, ssl => $ENV{MONGO_SSL});

    # test conn format
    is(
        exception { $conn = MongoDB::MongoClient->new("host" => "mongodb://localhost:27017", ssl => $ENV{MONGO_SSL}); },
        undef,
        'connected'
    );

    is(
        exception { $conn = MongoDB::MongoClient->new("host" => "mongodb://localhost:27017,", ssl => $ENV{MONGO_SSL}); },
        undef,
        'extra comma'
    );

    {
        is(
            exception {
                my $ip = 27020;
                while ((exists $ENV{DB_PORT} && $ip eq $ENV{DB_PORT}) ||
                    (exists $ENV{DB_PORT2} && $ip eq $ENV{DB_PORT2})) {
                    $ip++;
                }
                my $conn2 = MongoDB::MongoClient->new("host" => "mongodb://localhost:".$ip.",localhost:".($ip+1).",localhost", ssl => $ENV{MONGO_SSL});
            },
            undef,
            'last in line'
        );
    }

    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/example_db')->db_name, 'example_db', 'connection uri database');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost,/example_db')->db_name, 'example_db', 'connection uri database trailing comma');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/example_db?')->db_name, 'example_db', 'connection uri database trailing question');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost,localhost:27020,localhost:27021/example_db')->db_name, 'example_db', 'connection uri database, many hosts');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/?')->db_name, '', 'connection uri no database');
    is(MongoDB::MongoClient->new('host' => 'mongodb://:@localhost/?')->db_name, '', 'connection uri empty extras');
}

# get_database and drop 
{
    my $db = $conn->get_database($testdb->name);
    isa_ok($db, 'MongoDB::Database', 'get_database');

    $db->get_collection('test_collection')->insert_one({ foo => 42 });

    ok((grep { /testdb/ } $conn->database_names), 'database_names');

    my $result = $db->drop;
    is(ref $result, 'HASH', $result);
    is($result->{'ok'}, 1, 'db was dropped');
}


# TODO: won't work on master/slave until SERVER-2329 is fixed
# ok(!(grep { $_ eq 'test_database' } $conn->database_names), 'database got dropped');

subtest "wire protocol versions" => sub {
    is $conn->MIN_WIRE_VERSION, 0, 'default min wire version';
    is $conn->MAX_WIRE_VERSION, 3, 'default max wire version';

    # monkey patch wire versions
    my $conn2 = build_client();
    $conn2->_topology->{min_wire_version} = 100;
    $conn2->_topology->{max_wire_version} = 101;

    like(
        exception { $conn2->send_admin_command([is_master => 1]) },
        qr/Incompatible wire protocol/i,
        'exception on wire protocol'
    );

};

done_testing;
