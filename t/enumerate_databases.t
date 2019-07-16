#  Copyright 2017 - present MongoDB, Inc.
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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;
use MongoDB::Error;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    get_unique_collection
    skip_unless_min_version
/;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);

skip_unless_min_version($conn, 'v3.6.0');

subtest 'list databases' => sub {
    my @test_dbs;
    my $time_prefix = time();

    for my $prefix ( qw/ foo bar baz / ) {
        my $db1 = get_test_db( $conn, $prefix . $time_prefix );
        my $db2 = get_test_db( $conn, $prefix . $time_prefix );
        # getting a new db is not enough, must insert something into them first
        get_unique_collection( $db1, 'test' )->insert_one({ _id => 1 });
        get_unique_collection( $db2, 'test' )->insert_one({ _id => 1 });
        push @test_dbs, $db1, $db2;
    }
    my @all_dbs = $conn->list_databases;

    ok( scalar( @all_dbs ) >= 6, "Found at least 6 databases" );

    my @foo_dbs = $conn->list_databases({ filter => { name => qr/^foo${\$time_prefix}/ } });

    is( scalar( @foo_dbs ), 2, "Found two foo databases" );

    for my $foo_db ( @foo_dbs ) {
        ok( exists $foo_db->{empty}, "Database has empty attribute" );
        ok( $foo_db->{name} =~ /^foo${\$time_prefix}/, "Database has correct name" );
        ok( exists $foo_db->{sizeOnDisk}, "Database has sizeOnDisk attribute" );
    }

    for my $db ( @test_dbs ) {
        $db->drop;
    }
};

subtest 'list database names' => sub {
    my @test_dbs;
    my @test_db_names;
    my $time_prefix = time();

    for my $prefix ( qw/ foo bar baz / ) {
        my $db1 = get_test_db( $conn, $prefix . $time_prefix );
        my $db2 = get_test_db( $conn, $prefix . $time_prefix );
        # getting a new db is not enough, must insert something into them first
        get_unique_collection( $db1, 'test' )->insert_one({ _id => 1 });
        get_unique_collection( $db2, 'test' )->insert_one({ _id => 1 });
        push @test_dbs, $db1, $db2;
        push @test_db_names, $db1->{name}, $db2->{name};
    }

    my @all_names = $conn->database_names({ filter => { name => qr/^(foo|bar|baz)${\$time_prefix}/ } });

    my @sorted_test_db_names = sort @test_db_names;
    is_deeply( \@all_names, \@sorted_test_db_names, "Got expected set of names" );

    for my $db ( @test_dbs ) {
        $db->drop;
    }
};

done_testing;
