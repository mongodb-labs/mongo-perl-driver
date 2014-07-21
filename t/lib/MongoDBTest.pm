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

our @EXPORT_OK = ( 'build_client', 'get_test_db', 'server_version', 'server_type' );
my @testdbs;

# abstract building a connection
sub build_client {
    my @args = @_;
    my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';

    return MongoDB::MongoClient->new(
        host => $host, ssl => $ENV{MONGO_SSL}, find_master => 1, @args,
    );
}

sub get_test_db {

    my $conn = shift;
    my $testdb = 'testdb' . int(rand(2**31));
    my $db = $conn->get_database($testdb) or die "Can't get database\n";
    push(@testdbs, $db);
    return  $db;
}

sub server_version {

    my $conn = shift;
    my $build = $conn->get_database( 'admin' )->get_collection( '$cmd' )->find_one( { buildInfo => 1 } );
    my ($version_str) = $build->{version} =~ m{^([0-9.]+)};
    return version->parse("v$version_str");
}

sub server_type {

    my $conn = shift;
    my $server_type;

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
}

# cleanup test dbs
END {
    for my $db (@testdbs) {
        $db->drop;
    }
}

1;
