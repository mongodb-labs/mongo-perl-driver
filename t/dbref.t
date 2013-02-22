use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use Scalar::Util 'blessed';

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::MongoClient->new(host => $host, ssl => $ENV{MONGO_SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 8;
}

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
