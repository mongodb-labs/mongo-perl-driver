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



package MongoDBTest;

use strict;
use warnings;

use Exporter 'import';
use MongoDB;
use Test::More;
use version;

our @EXPORT_OK = ( '$conn', '$testdb', '$using_2_6', '$server_type' );
our $conn;
our $testdb;
our $using_2_6;
our $server_type;

# set up connection to a test database if we can
BEGIN { 
    eval { 
        my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';
        $conn = MongoDB::MongoClient->new(
            host => $host, ssl => $ENV{MONGO_SSL}, find_master => 1
        );
        $testdb = $conn->get_database('testdb' . time()) or
            die "Can't get database";
    };

    if ( $@ ) { 
        plan skip_all => $@;
        exit 0;
    }
};

# check database version
my $build = $conn->get_database( 'admin' )->get_collection( '$cmd' )->find_one( { buildInfo => 1 } );
my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
$using_2_6 = version->parse("v$version_str") >= v2.5.5;

# check database type
my $ismaster = $conn->get_database('admin')->_try_run_command({ismaster => 1});
if (exists $ismaster->{msg} && $ismaster->{msg} eq 'isdbgrid') {
    $server_type = 'Mongos';
}
elsif ( $ismaster->{ismaster} && exists $ismaster->{setName} ) {
    $server_type = 'RSPrimary'
}
elsif ( ! exists $ismaster->{setName} && ! $ismaster->{isreplicaset} ) {
    $server_type = 'Standalone'
}
else {
    $server_type = 'Unknown';
}

# clean up any detritus from failed tests
END { 
    return unless $testdb;

    $testdb->drop;
};

1;
