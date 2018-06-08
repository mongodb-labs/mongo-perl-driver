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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    clear_testdbs
    get_unique_collection
    server_version
    server_type
    check_min_server_version
    skip_unless_mongod
    skip_unless_failpoints_available
    get_features
/;

skip_unless_mongod();
skip_unless_failpoints_available();

my $conn = build_client(
    retry_writes => 1,
);
my $testdb         = get_test_db($conn);
my $coll = get_unique_collection( $testdb, 'retry_split_batch' );
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $features       = get_features($conn);

plan skip_all => "retryableWrites not supported on this MongoDB"
    unless ( $features->supports_retryWrites );

subtest "ordered batch split on size" => sub {

    enable_failpoint( { mode => { skip => 1 } } );

    {
        # 10MB (ish) doc
        my $big_string = "a" x ( 1024 * 1024 * 10 );
        # Done with 5 to blow past both the 16MB maxBsonObjectSize and maxMessageSizeBytes
        my $ret = $coll->insert_many( [ map { { _id => $_, a => $big_string } } 0 .. 5 ] );

        is scalar( @{ $ret->inserted } ), 6, 'successfully inserted 6 items';
    }
    disable_failpoint();

    is( $coll->count_documents, 6, "collection count" );

    enable_failpoint( { mode => { skip => 2 } } );
    {
        my $big_string_b = "b" x ( 1024 * 1024 * 10 );

        my $ret = $coll->bulk_write( [
            { update_one => [ { _id => 1 }, { '$set' => { a => $big_string_b } } ] },
            { update_one => [ { _id => 3 }, { '$set' => { a => $big_string_b } } ] },
            { update_one => [ { _id => 5 }, { '$set' => { a => $big_string_b } } ] },
        ] );

        is $ret->modified_count, 3, 'successfully modified 3 items';
    }
    disable_failpoint();

    is( $coll->count_documents, 6, "collection count" );
};

sub enable_failpoint {
    my $doc = shift;
    $conn->send_admin_command([
        configureFailPoint => 'onPrimaryTransactionalWrite',
        %$doc,
    ]);
}

sub disable_failpoint {
    my $doc = shift;
    $conn->send_admin_command([
        configureFailPoint => 'onPrimaryTransactionalWrite',
        mode => 'off',
    ]);
}

done_testing;
