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
use MongoDBTest qw/build_client get_test_db clear_testdbs server_version/;

sub _test_lock_unlock {
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );

    my $server_version = server_version($conn);

    # Lock
    my $ret = $conn->fsync({lock => 1});
    is($ret->{ok},              1, "fsync + lock returned 'ok' => 1");
    is(exists $ret->{seeAlso},  1, "fsync + lock returned a link to fsync+lock documentation.");
    is($ret->{info}, "now locked against writes, use db.fsyncUnlock() to unlock", "Successfully locked mongodb.");

    # Check the lock.
    if ($server_version <= v3.1.0) {
        $ret = $conn->get_database('admin')->get_collection('$cmd.sys.inprog')->find_one();
    } 
    else { 
        $ret = $conn->send_admin_command([currentOp => 1]);
        $ret = $ret->{output};
    }
    is($ret->{fsyncLock}, 1, "MongoDB is still locked.");
    is($ret->{info}, "use db.fsyncUnlock() to terminate the fsync write/snapshot lock", "Got docs on how to unlock (via shell).");

    # Unlock
    $ret = $conn->fsync_unlock();
    is($ret->{ok}, 1, "Got 'ok' => 1 from unlock command.");
    is($ret->{info}, "unlock completed", "Got a successful unlock.");
}

subtest "wire protocol 4" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-any.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_lock_unlock();

    ok( scalar $orc->get_server('host1')->grep_log(qr/command: fsyncUnlock/),
        "saw fsyncUnlock in log" );
};

subtest "wire protocol 3" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_lock_unlock();

    ok( !scalar $orc->get_server('host1')->grep_log(qr/command: fsyncUnlock/),
        "no fsyncUnlock in log" );
};

clear_testdbs;

done_testing;

