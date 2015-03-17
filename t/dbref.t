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
use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client();
my $testdb = get_test_db($conn);

plan tests => 28;


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
}

# test fetch
{ 
    $testdb->get_collection( 'test_coll' )->insert_one( { _id => 123, foo => 'bar' } );

    my $ref = MongoDB::DBRef->new( db => 'fake_db_does_not_exist', 'ref', 'fake_coll_does_not_exist', id => 123 );
    like(
        exception { $ref->fetch },
        qr/Can't fetch DBRef without a MongoClient/,
        "fetch without dbref throws exception"
    );

    $ref->client( $conn );
    like( exception { $ref->fetch }, qr/No such database fake_db_does_not_exist/, "db doesn't exist throws" );

    $ref->db( $testdb->name );
    like( exception { $ref->fetch }, qr/No such collection fake_coll_does_not_exist/, "collection doesn't exist throws" );

    $ref->ref( 'test_coll' );
    
    my $doc = $ref->fetch;
    is $doc->{_id}, 123;
    is $doc->{foo}, 'bar';

    $testdb->get_collection( 'test_coll' )->drop;
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

    $coll->drop;
}

# test fetch via find
{
    my $some_coll = $testdb->get_collection( 'some_coll' );
    $some_coll->insert_one( { _id => 123, value => 'foobar' } );
    my $dbref = MongoDB::DBRef->new( db => $testdb->name, ref => 'some_coll', id => 123 );

    my $coll = $testdb->get_collection( 'test_coll' );
    $coll->insert_one( { _id => 'wut wut wut', thing => $dbref } );

    my $ref_doc = $coll->find_one( { _id => 'wut wut wut' } )->{thing}->fetch;

    ok $ref_doc;
    is $ref_doc->{_id}, 123;
    is $ref_doc->{value}, 'foobar';

    $coll->drop;
    $some_coll->drop;
}

# test inflate_dbrefs flag
{
    $conn->inflate_dbrefs( 0 );
    my $dbref = MongoDB::DBRef->new( db => $testdb->name, ref => 'some_coll', id => 123 );

    my $coll = $testdb->get_collection( 'test_coll' );
    $coll->insert_one( { _id => 'wut wut wut', thing => $dbref } );

    my $doc = $coll->find_one( { _id => 'wut wut wut' } );
    ok exists $doc->{thing};
    ok ref $doc->{thing};
    ok reftype $doc->{thing} eq reftype { };
    ok not blessed $doc->{thing};
    is $doc->{thing}{'$db'}, $testdb->name;
    is $doc->{thing}{'$ref'}, 'some_coll';
    is $doc->{thing}{'$id'}, 123;

    $coll->drop;
}
