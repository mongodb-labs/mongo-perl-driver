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

our @EXPORT_OK = ( '$conn', '$testdb', '$server_type', '$server_version' );
our $conn;
our $testdb;
our $server_type;
our $server_version;

# abstract building a connection
sub build_client {
    my @args = @_;
    my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';
    return MongoDB::MongoClient->new(
        host => $host, ssl => $ENV{MONGO_SSL}, find_master => 1, @args,
    );
}

# set up connection to a test database if we can
BEGIN { 
    eval { 
        $conn = build_client();
        $testdb = $conn->get_database('testdb' . int(rand(2**31))) or
            die "Can't get database\n";
        eval { $conn->get_database("admin")->_try_run_command({ serverStatus => 1 }) }
            or die "Database has auth enabled\n";
    };

    if ( $@ ) { 
        plan skip_all => $@;
        exit 0;
    }
};

# check database version
my $build = $conn->get_database( 'admin' )->get_collection( '$cmd' )->find_one( { buildInfo => 1 } );
my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
$server_version = version->parse("v$version_str");

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
