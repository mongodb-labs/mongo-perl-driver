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
use Test::Exception;
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;

use lib "t/lib";
use MongoDBTest '$conn';

plan tests => 27;

throws_ok {
    MongoDB::MongoClient->new(host => 'localhost', port => 1, ssl => $ENV{MONGO_SSL});
} qr/couldn't connect to server/, 'exception on connection failure';

SKIP: {
    skip "connecting to default host/port won't work with a remote db", 13 if exists $ENV{MONGOD};

    lives_ok {
        $conn = MongoDB::MongoClient->new(ssl => $ENV{MONGO_SSL});
    } 'successful connection';
    isa_ok($conn, 'MongoDB::MongoClient');

    is($conn->host, 'mongodb://localhost:27017', 'host default value');
    is($conn->db_name, 'admin', 'db_name default value');

    # just make sure a couple timeouts work
    my $to = MongoDB::MongoClient->new('timeout' => 1, ssl => $ENV{MONGO_SSL});
    $to = MongoDB::MongoClient->new('timeout' => 123, ssl => $ENV{MONGO_SSL});
    $to = MongoDB::MongoClient->new('timeout' => 2000000, ssl => $ENV{MONGO_SSL});

    # test conn format
    lives_ok {
        $conn = MongoDB::MongoClient->new("host" => "mongodb://localhost:27017", ssl => $ENV{MONGO_SSL});
    } 'connected';

    lives_ok {
        $conn = MongoDB::MongoClient->new("host" => "mongodb://localhost:27017,", ssl => $ENV{MONGO_SSL});
    } 'extra comma';

    lives_ok {
        my $ip = 27020;
        while ((exists $ENV{DB_PORT} && $ip eq $ENV{DB_PORT}) ||
               (exists $ENV{DB_PORT2} && $ip eq $ENV{DB_PORT2})) {
            $ip++;
        }
        my $conn2 = MongoDB::MongoClient->new("host" => "mongodb://localhost:".$ip.",localhost:".($ip+1).",localhost", ssl => $ENV{MONGO_SSL});
    } 'last in line';

    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/example_db')->db_name, 'example_db', 'connection uri database');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost,/example_db')->db_name, 'example_db', 'connection uri database trailing comma');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/example_db?')->db_name, 'example_db', 'connection uri database trailing question');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost:27020,localhost:27021,localhost/example_db')->db_name, 'example_db', 'connection uri database, many hosts');
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/?')->db_name, 'admin', 'connection uri no database');
    is(MongoDB::MongoClient->new('host' => 'mongodb://:@localhost/?')->db_name, 'admin', 'connection uri empty extras');
}

my $db = $conn->get_database('test_database');
isa_ok($db, 'MongoDB::Database', 'get_database');

$db->get_collection('test_collection')->insert({ foo => 42 }, {safe => 1});

ok((grep { $_ eq 'test_database' } $conn->database_names), 'database_names');

my $result = $db->drop;
is(ref $result, 'HASH', $result);
is($result->{'ok'}, 1, 'db was dropped');

# TODO: won't work on master/slave until SERVER-2329 is fixed
# ok(!(grep { $_ eq 'test_database' } $conn->database_names), 'database got dropped');


# w
{
    is($conn->w, 1, "get w");
    $conn->w(3);
    is($conn->w, 3, "set w");

    $conn->w("tag");
    is($conn->w, "tag", "set w to string");

    dies_ok { $conn->w({tag => 1});} "Setting w to anything but a string or int dies.";

    is($conn->wtimeout, 1000, "get wtimeout");
    $conn->wtimeout(100);
    is($conn->wtimeout, 100, "set wtimeout");

    $db->drop;
}


# query_timeout
{
    my $timeout = $MongoDB::Cursor::timeout;

    my $conn2 = MongoDB::MongoClient->new(auto_connect => 0, ssl => $ENV{MONGO_SSL});
    is($conn2->query_timeout, $timeout, 'query timeout');

    $MongoDB::Cursor::timeout = 40;
    $conn2 = MongoDB::MongoClient->new(auto_connect => 0, ssl => $ENV{MONGO_SSL});
    is($conn2->query_timeout, 40, 'query timeout');

    $MongoDB::Cursor::timeout = $timeout;
}

# max_bson_size
{
    my $size = $conn->max_bson_size;
    my $result = $conn->get_database( 'admin' )->run_command({buildinfo => 1});
    if (exists $result->{'maxBsonObjectSize'}) {
        is($size, $result->{'maxBsonObjectSize'});
    }
    else {
        is($size, 4*1024*1024);
    }
}

END {
    if ($conn) {
        $conn->get_database( 'foo' )->drop;
    }
    if ($db) {
        $db->drop;
    }
}
