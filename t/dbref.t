use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use Scalar::Util 'blessed', 'reftype';

use lib "t/lib";
use MongoDBTest '$conn';

plan tests => 28;


{
    my $ref = MongoDB::DBRef->new( db => 'test', ref => 'test_coll', id => 123 );
    ok $ref;
    isa_ok $ref, 'MongoDB::DBRef';
}

# test type coercions 
{ 
    my $db   = $conn->get_database( 'test' );
    my $coll = $db->get_collection( 'test_collection' );

    my $ref = MongoDB::DBRef->new( db => $db, ref => $coll, id => 123 );

    ok $ref;
    ok not blessed $ref->db;
    ok not blessed $ref->ref;

    is $ref->db, 'test';
    is $ref->ref, 'test_collection';
    is $ref->id, 123;
}

# test fetch
{ 
    $conn->get_database( 'test' )->get_collection( 'test_coll' )->insert( { _id => 123, foo => 'bar' } );

    my $ref = MongoDB::DBRef->new( db => 'fake_db_does_not_exist', 'ref', 'fake_coll_does_not_exist', id => 123 );
    throws_ok { $ref->fetch } qr/Can't fetch DBRef without a MongoClient/;

    $ref->client( $conn );
    throws_ok { $ref->fetch } qr/No such database fake_db_does_not_exist/;

    $ref->db( 'test' );
    throws_ok { $ref->fetch } qr/No such collection fake_coll_does_not_exist/;

    $ref->ref( 'test_coll' );
    
    my $doc = $ref->fetch;
    is $doc->{_id}, 123;
    is $doc->{foo}, 'bar';

    $conn->get_database( 'test' )->get_collection( 'test_coll' )->drop;
}

# test roundtrip
{
    my $dbref = MongoDB::DBRef->new( db => 'some_db', ref => 'some_coll', id => 123 );
    my $coll = $conn->get_database( 'test' )->get_collection( 'test_coll' );

    $coll->insert( { _id => 'wut wut wut', thing => $dbref } );

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
    my $some_coll = $conn->get_database( 'test' )->get_collection( 'some_coll' );
    $some_coll->insert( { _id => 123, value => 'foobar' } );
    my $dbref = MongoDB::DBRef->new( db => 'test', ref => 'some_coll', id => 123 );

    my $coll = $conn->get_database( 'test' )->get_collection( 'test_coll' );
    $coll->insert( { _id => 'wut wut wut', thing => $dbref } );

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
    my $dbref = MongoDB::DBRef->new( db => 'test', ref => 'some_coll', id => 123 );

    my $coll = $conn->get_database( 'test' )->get_collection( 'test_coll' );
    $coll->insert( { _id => 'wut wut wut', thing => $dbref } );

    my $doc = $coll->find_one( { _id => 'wut wut wut' } );
    ok exists $doc->{thing};
    ok ref $doc->{thing};
    ok reftype $doc->{thing} eq reftype { };
    ok not blessed $doc->{thing};
    is $doc->{thing}{'$db'}, 'test';
    is $doc->{thing}{'$ref'}, 'some_coll';
    is $doc->{thing}{'$id'}, 123;

    $coll->drop;
}
