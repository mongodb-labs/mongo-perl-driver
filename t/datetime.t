use strict;
use warnings;

use Test::More tests => 13;

use MongoDB;
use MongoDB::DateTime;
use DateTime;
use MongoDB::BSON;

my $now  = time;
my $conn = MongoDB::Connection->new();
my $db   = $conn->get_database('foo');
my $c    = $db->get_collection('bar');
my $dt   = DateTime->from_epoch( epoch => $now );
my $md   = MongoDB::DateTime->from_epoch( epoch => $now );

is($MongoDB::BSON::use_mongodb_datetime, 0, 'default value ok');

# Test the MongoDB::DateTime module
{
    my $d1 = MongoDB::DateTime->from_epoch( epoch => $now );
    my $d2 = MongoDB::DateTime->from_epoch( epoch => $now - 3600 );
    isa_ok( $d1, 'MongoDB::DateTime' );
    ok( $d1 > $d2, 'overload works on comparisons' );
    is( $d1 - $d2, 3600,   'ovreload works on aritmetic ops' );
    is( "$d1",     "$now", 'overload works on strings' );
}

# DateTime
{
    $c->drop;
    $c->insert( { dt => $dt, md => $md } );
    my $r = $c->find_one;
    isa_ok( $r->{dt}, 'DateTime' );
    isa_ok( $r->{md}, 'DateTime' );
    is( $r->{dt}->epoch, $now, 'the time is right for dt' );
    is( $r->{md}->epoch, $now, 'the time is right for md' );
}

# MongoDB::DateTime
{
    $MongoDB::BSON::use_mongodb_datetime = 1;
    $c->drop;
    $c->insert( { dt => $dt, md => $md } );
    my $r = $c->find_one;
    isa_ok( $r->{dt}, 'MongoDB::DateTime' );
    isa_ok( $r->{md}, 'MongoDB::DateTime' );
    is( $r->{dt}->epoch, $now, 'MongoDB::DateTime - the time is right for dt' );
    is( $r->{md}->epoch, $now, 'MongoDB::DateTime - the time is right for md' );
}

$db->drop;

