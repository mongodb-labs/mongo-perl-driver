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

like(
    exception { MongoDB::MongoClient->new(host => 'localhost', port => 1, ssl => $ENV{MONGO_SSL}); },
    qr/could not connect/i,
    'exception on connection failure'
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
    is($conn->db_name, 'admin', 'db_name default value');

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

    TODO: {
        local $TODO = "pending proper server selection";
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
    is(MongoDB::MongoClient->new('host' => 'mongodb://localhost/?')->db_name, 'admin', 'connection uri no database');
    is(MongoDB::MongoClient->new('host' => 'mongodb://:@localhost/?')->db_name, 'admin', 'connection uri empty extras');
}

# get_database and drop 
{
    my $db = $conn->get_database($testdb->name);
    isa_ok($db, 'MongoDB::Database', 'get_database');

    $db->get_collection('test_collection')->insert({ foo => 42 }, {safe => 1});

    ok((grep { /testdb/ } $conn->database_names), 'database_names');

    my $result = $db->drop;
    is(ref $result, 'HASH', $result);
    is($result->{'ok'}, 1, 'db was dropped');
}


# TODO: won't work on master/slave until SERVER-2329 is fixed
# ok(!(grep { $_ eq 'test_database' } $conn->database_names), 'database got dropped');


# w
{
    is($conn->w, 1, "get w");
    $conn->w(3);
    is($conn->w, 3, "set w");

    $conn->w("tag");
    is($conn->w, "tag", "set w to string");

    isnt(
        exception { $conn->w({tag => 1});},
        undef,
        "Setting w to anything but a string or int dies."
    );

    is($conn->wtimeout, 1000, "get wtimeout");
    $conn->wtimeout(100);
    is($conn->wtimeout, 100, "set wtimeout");

    $testdb->drop;
}

subtest "options" => sub {

    subtest "connection" => sub {

        my $ssl = "true";
        my $timeout = 40000;
        my $client = MongoDB::MongoClient->new({host => "mongodb://localhost/?ssl=$ssl&connectTimeoutMS=$timeout", auto_connect => 0});

        is( $client->ssl, 1, "connect with ssl set" );
        is( $client->timeout, $timeout, "connection timeout set" );
    };

    subtest "invalid option value" => sub {

        like(
            exception { MongoDB::MongoClient->new({host => "mongodb://localhost/?ssl=", auto_connect => 0}) },
            qr/expected key value pair/,
            'key should have value'
        );
    };

    subtest "write concern" => sub {

        my $w = 2;
        my $wtimeout = 200;
        my $j = "true";
        my $client = MongoDB::MongoClient->new({host => "mongodb://localhost/?w=$w&wtimeoutMS=$wtimeout&journal=$j", auto_connect => 0});

        is( $client->w, $w, "write acknowledgement set" );
        is( $client->wtimeout, $wtimeout, "write acknowledgement timeout set" );
        is( $client->j, 1, "sync to journal" );
    };
};


# query_timeout
{
    my $client = MongoDB::MongoClient->new(auto_connect => 0);
    is($client->query_timeout, $MongoDB::Cursor::timeout, 'default query timeout');

    local $MongoDB::Cursor::timeout = 40;
    $client = MongoDB::MongoClient->new(auto_connect => 0);
    is($client->query_timeout, 40, 'changed default query timeout');
}

# max_bson_size
TODO: {
    local $TODO = "pending cluster monitoring";
    my $size = $conn->max_bson_size;
    my $result = $conn->get_database( 'admin' )->run_command({buildinfo => 1});
    if (exists $result->{'maxBsonObjectSize'}) {
        is($size, $result->{'maxBsonObjectSize'}, 'max bson size');
    }
    else {
        is($size, 4*1024*1024, 'max bson size');
    }
}

# wire protocol versions

TODO: {
    local $TODO = "pending cluster monitoring";

    is $conn->min_wire_version, 0, 'default min wire version';
    is $conn->max_wire_version, 2, 'default max wire version';

    like(
        exception { MongoDBTest::build_client( min_wire_version => 99, max_wire_version => 100) },
        qr/Incompatible wire protocol/i,
        'exception on wire protocol'
    );

}

done_testing;
