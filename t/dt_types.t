use strict;
use warnings;
use Test::More;
use Test::Exception;
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;
use DateTime;
use DateTime::Tiny;

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
    plan tests => 7;
}

my $db = $conn->get_database('test_database');
$db->drop;

my $now = DateTime->now;
{
    $db->get_collection( 'test_collection' )->insert( { date => $now } );

    my $date1 = $db->get_collection( 'test_collection' )->find_one->{date};
    isa_ok $date1, 'DateTime';
    is $date1->epoch, $now->epoch;
    $db->drop;
}

{
    $db->get_collection( 'test_collection' )->insert( { date => $now } );
    $conn->dt_type( undef );
    my $date3 = $db->get_collection( 'test_collection' )->find_one->{date};
    ok( not ref $date3 );
    is $date3, $now->epoch;
    $db->drop;
}


{
    $db->get_collection( 'test_collection' )->insert( { date => $now } );
    $conn->dt_type( 'DateTime::Tiny' );
    my $date2 = $db->get_collection( 'test_collection' )->find_one->{date};
    isa_ok( $date2, 'DateTime::Tiny' );
    is $date2->DateTime->epoch, $now->epoch;
    $db->drop;
}

{
    $db->get_collection( 'test_collection' )->insert( { date => $now } );
    $conn->dt_type( 'DateTime::Bad' );
    throws_ok { 
        my $date4 = $db->get_collection( 'test_collection' )->find_one->{date};
    } qr/Invalid dt_type "DateTime::Bad"/i;

}

