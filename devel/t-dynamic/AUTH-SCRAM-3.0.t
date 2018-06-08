#  Copyright 2018 - present MongoDB, Inc.
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
use MongoDB::_URI;

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/mongod-3.0-scram.yml" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;

my $uri = MongoDB::_URI->new( uri => $ENV{MONGOD} );
my $no_auth_string = "mongodb://" . $uri->hostids->[0];

subtest "no authentication" => sub {
    my $conn   = build_client( host => $no_auth_string, dt_type => undef );
    my $testdb = get_test_db($conn);
    my $coll   = $testdb->get_collection("test_collection");

    like(
        exception { $coll->count_documents },
        qr/not authorized/,
        "can't read collection when not authenticated"
    );
};

subtest "MONGODB-CR disabled" => sub {

    like(
        exception {
            my $conn = build_client(
                host     => $no_auth_string,
                username => $uri->username,
                password => $uri->password,
                db_name  => $uri->db_name,
                dt_type  => undef,
                auth_mechanism => 'MONGODB-CR',
            );

            my $testdb = get_test_db($conn);
            my $coll   = $testdb->get_collection("test_collection");
            $coll->count_documents();
        },

        qr/challenge-response.*disabled/i,
        "can't read collection using MONGODB-CR"
    );
};

subtest "auth via client attributes" => sub {
    my $conn = build_client(
        host     => $no_auth_string,
        username => $uri->username,
        password => $uri->password,
        db_name  => $uri->db_name,
        dt_type  => undef,
        auth_mechanism => 'SCRAM-SHA-1',
    );
    my $testdb = get_test_db($conn);
    my $coll   = $testdb->get_collection("test_collection");

    is( exception { $coll->count_documents }, undef, "no exception reading from new client" );
};

subtest "auth via connection string" => sub {
    my $conn   = build_client( dt_type => undef );
    my $testdb = get_test_db($conn);
    my $coll   = $testdb->get_collection("test_collection");

    is( exception { $coll->count_documents }, undef, "no exception reading from new client" );
};

clear_testdbs;

done_testing;
