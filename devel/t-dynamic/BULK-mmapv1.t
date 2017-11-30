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

my $orc =
    MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-any-mmapv1.yml" );
diag "starting cluster";
$orc->start;
local $ENV{MONGOD} = $orc->as_uri;

my $conn = build_client( dt_type => undef );
my $admin  = $conn->get_database("admin");
my $testdb = get_test_db($conn);
my $coll   = $testdb->get_collection("test_collection");
my $server_status = $admin->run_command([serverStatus => 1]);

note("NO JOURNAL");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: no journal" => sub {
        plan skip_all => 'needs a standalone server without journaling'
          unless !exists $server_status->{dur};

        $coll->drop;
        my $bulk = $coll->$method;
        $bulk->insert_one( {} );
        my $err = exception { $bulk->execute( { j => 1 } ) };
        isa_ok( $err, 'MongoDB::DatabaseError', "executing j:1 on nojournal throws error" );
        like( $err->message, qr/journal/, "error message mentions journal" );
    };
}

clear_testdbs;

done_testing;

