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
use Test::More;
use Test::Fatal;

use MongoDB;
use Scalar::Util 'blessed', 'reftype';

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);

{
    ok( my $ref = BSON::DBRef->new( db => 'test', ref => 'test_coll', id => 123 ), "constructor" );
    isa_ok( $ref, 'BSON::DBRef' );
}

# test type coercions
{
    my $coll = $testdb->get_collection( 'test_collection' );

    my $ref = BSON::DBRef->new( db => $testdb->name, ref => $coll, id => 123 );

    ok $ref;
    ok not blessed $ref->db;
    ok not blessed $ref->ref;

    is $ref->db, $testdb->name;
    is $ref->ref, 'test_collection';
    is $ref->id, 123;

    $ref = BSON::DBRef->new( ref => $coll, id => 123 );
    is( $ref->db, undef, "no db in new gives undef db" );

    $ref = BSON::DBRef->new( ref => $coll, id => 123, db => undef );
    is( $ref->db, undef, "explicit undef db in new gives undef db" );
}

# test roundtrip
{
    my $dbref = BSON::DBRef->new( db => 'some_db', ref => 'some_coll', id => 123 );
    my $coll = $testdb->get_collection( 'test_coll' );

    $coll->insert_one( { _id => 'wut wut wut', thing => $dbref } );

    my $doc = $coll->find_one( { _id => 'wut wut wut' } );
    ok exists $doc->{thing};

    my $thing = $doc->{thing};

    isa_ok $thing, 'BSON::DBRef';
    is $thing->ref, 'some_coll';
    is $thing->id,  123;
    is $thing->db,  'some_db';

    $dbref = BSON::DBRef->new( ref => 'some_coll', id => 123 );
    $coll->insert_one( { _id => 123, thing => $dbref } );
    $doc = $coll->find_one( { _id => 123 } );
    $thing = $doc->{thing};
    isa_ok( $thing, 'BSON::DBRef' );
    is( $thing->ref, 'some_coll', '$ref' );
    is( $thing->id,  123,         '$id' );
    is( $thing->db,  undef,       '$db undefined' );

    $coll->drop;
}

# test round-tripping extra fields
subtest "round-trip fields" => sub {
    my $coll = $testdb->get_collection( 'test_coll' );
    $coll->drop;

    my $ixhash = Tie::IxHash->new(
        '$ref' => 'some_coll',
        '$id'  => 456,
        foo    => 'bar',
        baz    => 'bam',
        id     => '123', # should be OK, since $id is taken first
    );

    $coll->insert_one( { _id => 123, thing => $ixhash } );

    my $doc = $coll->find_one( { _id => 123 } );
    my $dbref = $doc->{thing};

    isa_ok( $dbref, "BSON::DBRef" );

    $coll->insert_one( { _id => 124, thing => $dbref } );
    $doc = $coll->find_one( { _id => 124 } );
    $dbref = $doc->{thing};

    for my $k ( $ixhash->Keys ) {
        next if $k =~ /^\$/;
        is( $dbref->extra->{$k}, $ixhash->FETCH($k), "$k" );
    }

};

done_testing;

# vim: set ts=4 sts=4 sw=4 et tw=75:
