#
#  Copyright 2015 MongoDB, Inc.
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
use Test::Fatal;
use Test::Warn;
use Test::Deep qw/!blessed/;

use Tie::IxHash;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_version server_type get_capped/;

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

my ($iv);

subtest "collection API" => sub {
    $iv = $coll->indexes;
    isa_ok( $iv, "MongoDB::IndexView", "coll->indexes" );
};

subtest "create_many" => sub {
    $coll->drop;
    my @names = $iv->create_many( { keys => [ x => 1 ] }, { keys => [ y => -1 ] } );
    ok( scalar @names, "got non-empty result" );
    is_deeply( [ sort @names ], [ sort qw/x_1 y_-1/ ], "returned list of names" );

    # exception on index that exist
    like(
        exception {
            $iv->create_many( { keys => [ x => 1 ] },
                { keys => [ x => 1 ], options => { unique => 1 } } )
        },
        qr/MongoDB::DatabaseError/,
        "exception on existing index",
    );
};

subtest "list indexes" => sub {
    $coll->drop;
    $coll->insert( {} );
    my $res = $iv->list();
    isa_ok( $res, "MongoDB::QueryResult", "indexes->list" );
    is_deeply( [ sort map { $_->{name} } $res->all ],
        ['_id_'], "list only gives _id_ index" );
    ok( $iv->create_many( { keys => [ x => 1 ] } ), "added index" );
    is_deeply(
        [ sort map { $_->{name} } $iv->list->all ],
        [ sort qw/_id_ x_1/ ],
        "list finds both indexes"
    );
};

done_testing;

# vim: set ts=4 sts=4 sw=4 et tw=75:
