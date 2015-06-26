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
use MongoDB::BSON;
use Scalar::Util 'blessed', 'reftype';

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client();
my $testdb = get_test_db($conn);

{
    my $ref = MongoDB::DBRef->new( db => 'test', ref => 'test_coll', id => 123 );
    ok $ref;
    isa_ok $ref, 'MongoDB::DBRef';
}

# test type coercions
{
    my $coll = $testdb->get_collection( 'test_collection' );

    my $ref = MongoDB::DBRef->new( db => $testdb, ref => $coll, id => 123 );

    ok $ref;
    ok not blessed $ref->db;
    ok not blessed $ref->ref;

    is $ref->db, $testdb->name;
    is $ref->ref, 'test_collection';
    is $ref->id, 123;

    $ref = MongoDB::DBRef->new( ref => $coll, id => 123 );
    is( $ref->db, undef, "no db in new gives undef db" );

    $ref = MongoDB::DBRef->new( ref => $coll, id => 123, db => undef );
    is( $ref->db, undef, "explicit undef db in new gives undef db" );
}

# test roundtrip
{
    my $dbref = MongoDB::DBRef->new( db => 'some_db', ref => 'some_coll', id => 123 );
    my $coll = $testdb->get_collection( 'test_coll' );

    $coll->insert_one( { _id => 'wut wut wut', thing => $dbref } );

    my $doc = $coll->find_one( { _id => 'wut wut wut' } );
    ok exists $doc->{thing};

    my $thing = $doc->{thing};

    isa_ok $thing, 'MongoDB::DBRef';
    is $thing->ref, 'some_coll';
    is $thing->id,  123;
    is $thing->db,  'some_db';

    $dbref = MongoDB::DBRef->new( ref => 'some_coll', id => 123 );
    $coll->insert_one( { _id => 123, thing => $dbref } );
    $doc = $coll->find_one( { _id => 123 } );
    $thing = $doc->{thing};
    isa_ok( $thing, 'MongoDB::DBRef' );
    is( $thing->ref, 'some_coll', '$ref' );
    is( $thing->id,  123,         '$id' );
    is( $thing->db,  undef,       '$db undefined' );

    $coll->drop;
}

# test changing dbref_callback on bson_codec
{
    my $coll =
      $testdb->get_collection( 'test_coll', { bson_codec => {} } );

    my $dbref = MongoDB::DBRef->new( db => $testdb->name, ref => 'some_coll', id => 123 );

    $coll->insert_one( { _id => 'wut wut wut', thing => $dbref } );
    my $doc = $coll->find_one( { _id => 'wut wut wut' } );
    ok( exists $doc->{thing}, "got inserted doc from db" );
    is( ref $doc->{thing}, 'HASH', "doc is hash, not object" );;
    is( $doc->{thing}{'$id'}, 123, '$id' );
    is( $doc->{thing}{'$ref'}, 'some_coll', '$ref' );
    is( $doc->{thing}{'$db'}, $testdb->name, '$db' );

    $dbref = MongoDB::DBRef->new( ref => 'some_coll', id => 123 );
    $coll->insert_one( { _id => 123, thing => $dbref } );
    $doc = $coll->find_one( { _id => 123 } );
    ok( exists $doc->{thing}, "got inserted doc from db" );
    is( $doc->{thing}{'$id'}, 123, '$id' );
    is( $doc->{thing}{'$ref'}, 'some_coll', '$ref' );
    ok( !exists($doc->{thing}{'$db'}), '$db not inserted' );

    $coll->drop;
}

done_testing;

# vim: set ts=4 sts=4 sw=4 et tw=75:
