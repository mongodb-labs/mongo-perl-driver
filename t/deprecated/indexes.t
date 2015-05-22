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
use Test::Deep qw/!blessed/;
use Test::Fatal;
use Test::Warn;

use utf8;
use JSON::MaybeXS;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_version server_type/;

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection("foo");

# basic indexes
subtest 'basic indexes' => sub {
    $coll->drop;

    $coll->drop;
    for ( my $i = 0; $i < 10; $i++ ) {
        $coll->insert_one( { 'x' => $i, 'z' => 3, 'w' => 4 } );
        $coll->insert_one( { 'x' => $i, 'y' => 2, 'z' => 3, 'w' => 4 } );
    }

    $coll->drop;
    ok( !$coll->get_indexes, 'no indexes yet' );

    my $indexes = Tie::IxHash->new( foo => 1, bar => 1, baz => 1 );
    ok( $coll->ensure_index($indexes) );

    my $err = $testdb->last_error;
    is( $err->{ok},  1 );
    is( $err->{err}, undef );

    $indexes = Tie::IxHash->new( foo => 1, bar => 1 );
    ok( $coll->ensure_index($indexes) );

    $coll->insert_one( { foo => 1, bar => 1, baz => 1, boo => 1 } );
    $coll->insert_one( { foo => 1, bar => 1, baz => 1, boo => 2 } );
    is( $coll->count, 2 );

    ok( $coll->ensure_index( { boo => 1 }, { unique => 1 } ) );

    eval { $coll->insert_one( { foo => 3, bar => 3, baz => 3, boo => 2 } ) };

    is( $coll->count, 2, 'unique index' );

    my @indexes = $coll->get_indexes;
    is( scalar @indexes, 4, 'three custom indexes and the default _id_ index' );
    my ($foobarbaz) = grep { $_->{name} eq 'foo_1_bar_1_baz_1' } @indexes;
    is_deeply( [ sort keys %{ $foobarbaz->{key} } ], [ sort qw/foo bar baz/ ], );
    my ($foobar) = grep { $_->{name} eq 'foo_1_bar_1' } @indexes;
    is_deeply( [ sort keys %{ $foobar->{key} } ], [ sort qw/foo bar/ ], );

    $coll->drop_index('foo_1_bar_1_baz_1');
    @indexes = $coll->get_indexes;
    is( scalar @indexes, 3 );
    ok( ( !scalar grep { $_->{name} eq 'foo_1_bar_1_baz_1' } @indexes ),
        "right index deleted" );

    $coll->drop;
    ok( !$coll->get_indexes, 'no indexes after dropping' );

    # make sure this still works
    $coll->ensure_index( { "foo" => 1 } );
    @indexes = $coll->get_indexes;
    is( scalar @indexes, 2, '1 custom index and the default _id_ index' );
};

# test ensure index with drop_dups
subtest 'drop dups' => sub {
    $coll->drop;

    $coll->insert_one( { foo => 1, bar => 1, baz => 1, boo => 1 } );
    $coll->insert_one( { foo => 1, bar => 1, baz => 1, boo => 2 } );
    is( $coll->count, 2 );

    eval { $coll->ensure_index( { foo => 1 }, { unique => 1 } ) };
    like( $@, qr/E11000/, "got expected error creating unique index with dups" );

    # prior to 2.7.5, drop_dups was respected
    if ( $server_version < v2.7.5 ) {
        ok( $coll->ensure_index( { foo => 1 }, { unique => 1, drop_dups => 1 } ) );
    }

};

# test new form of ensure index
subtest 'new form of ensure index' => sub {
    $coll->drop;

    ok( $coll->ensure_index( { foo => 1, bar => -1, baz => 1 } ) );
    ok( $coll->ensure_index( [ foo => 1, bar => 1 ] ) );

    $coll->insert_one( { foo => 1, bar => 1, baz => 1, boo => 1 } );
    $coll->insert_one( { foo => 1, bar => 1, baz => 1, boo => 2 } );
    is( $coll->count, 2 );

    # unique index
    $coll->ensure_index( { boo => 1 }, { unique => 1 } );
    eval { $coll->insert_one( { foo => 3, bar => 3, baz => 3, boo => 2 } ) };
    is( $coll->count, 2, 'unique index' );
};

subtest '2d index with options' => sub {
    $coll->drop;

    $coll->ensure_index( { loc => '2d' }, { bits => 32, sparse => 1 } );

    my ($index) = grep { $_->{name} eq 'loc_2d' } $coll->get_indexes;

    ok( $index,           "created 2d index" );
    ok( $index->{sparse}, "sparse option set on index" );
    is( $index->{bits}, 32, "bits option set on index" );
};

subtest 'ensure index arbitrary options' => sub {
    $coll->ensure_index( { wibble => 1 }, { notReallyAnOption => { foo => 1 } } );
    my ($index) = grep { $_->{name} eq 'wibble_1' } $coll->get_indexes;
    ok( $index, "created index" );
    cmp_deeply(
        $index->{notReallyAnOption},
        { foo => 1 },
        "arbitrary option set on index"
    );
};

done_testing;
