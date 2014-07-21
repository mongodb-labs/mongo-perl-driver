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
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use Log::Any::Adapter qw/Stderr/;

use MongoDBTest::Orchestrator;

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/clusters/mongod-2.6-auth.yml" );
diag "starting server with auth enabled";
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client( dt_type => undef );
my $admin  = $conn->get_database("admin");
my $testdb = get_test_db($conn);
my $coll   = $testdb->get_collection("test_collection");

note("CAP-490, user management tests");

is( $admin->get_collection('system.users')->find({})->count, 1, "1 (root) user in system.users");

for ( 1 .. 3 ) {
    is(
        exception { $testdb->_try_run_command( [ createUser => "limited$_", pwd => "limited", roles => [ "readWrite" ] ] ) },
        undef,
        "createUser $_",
    );
}

is( $admin->get_collection('system.users')->find({})->count, 4, "4 users in system.users");

my $info;
is(
    exception { $info = $testdb->_try_run_command( [ usersInfo => "limited1", showCredentials  => boolean::true ] ) },
    undef,
    "usersInfo",
);

my $old_pwd_hash = $info->{users}[0]{credentials}{'MONGODB-CR'};

is(
    exception { $testdb->_try_run_command( [ updateUser => "limited1", pwd => "limited0", roles => [ "readWrite" ] ] ) },
    undef,
    "updateUser",
);

$info = undef;
is(
    exception { $info = $testdb->_try_run_command( [ usersInfo => "limited1", showCredentials  => boolean::true ] ) },
    undef,
    "usersInfo",
);

isnt( $info->{users}[0]{credentials}{'MONGODB-CR'}, $old_pwd_hash, "password was changed");

is(
    exception { $testdb->_try_run_command( [ dropUser => "limited1" ] ) },
    undef,
    "dropUser",
);

is( $admin->get_collection('system.users')->find({})->count, 3, "3 users in system.users");

is(
    exception { $testdb->_try_run_command( [ dropAllUsersFromDatabase => "limited0" ] ) },
    undef,
    "dropAllUsersFromDatabase",
);

is( $admin->get_collection('system.users')->find({})->count, 1, "1 user left in system.users");

done_testing;
