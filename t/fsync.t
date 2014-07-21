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
use Test::More 0.96;

use Data::Dumper;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client/;

my $conn = build_client();

my $ret;

# Test normal fsync.
subtest "normal fsync" => sub {
    $ret = $conn->fsync();
    is($ret->{ok},              1, "fsync returned 'ok' => 1");
    is(exists $ret->{numFiles}, 1, "fsync returned 'numFiles'");
};

# Test async fsync.
subtest "async fsync" => sub {
    $ret = $conn->fsync({async => 1});
    plan skip_all => 'async not supported'
       if $ret =~ /exception:/ && $ret =~ /not supported/;

    ok( ref $ret eq 'HASH', "fsync command ran without error" )
        or diag $ret;

    if ( ref $ret eq 'HASH' ) {
        is($ret->{ok},              1, "fsync + async returned 'ok' => 1");
        is(exists $ret->{numFiles}, 1, "fsync + async returned 'numFiles'");
    }
};

# Test fsync with lock.
subtest "fsync with lock" => sub {
    plan skip_all => "lock not supported through mongos"
        if $conn->_is_mongos;

    # Lock
    $ret = $conn->fsync({lock => 1});
    is($ret->{ok},              1, "fsync + lock returned 'ok' => 1");
    is(exists $ret->{seeAlso},  1, "fsync + lock returned a link to fsync+lock documentation.");
    is($ret->{info}, "now locked against writes, use db.fsyncUnlock() to unlock", "Successfully locked mongodb.");

    # Check the lock.
    $ret = $conn->get_database('admin')->get_collection('$cmd.sys.inprog')->find_one();
    is($ret->{fsyncLock}, 1, "MongoDB is still locked.");
    is($ret->{info}, "use db.fsyncUnlock() to terminate the fsync write/snapshot lock", "Got docs on how to unlock (via shell).");

    # Unlock 
    $ret = $conn->fsync_unlock(); Dumper($ret);
    is($ret->{ok}, 1, "Got 'ok' => 1 from unlock command.");
    is($ret->{info}, "unlock completed", "Got a successful unlock.");
};

done_testing;
