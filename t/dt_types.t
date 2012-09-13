use strict;
use warnings;
use Test::More;
use Test::Exception;
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;
use DateTime;

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::Connection->new(host => $host, ssl => $ENV{MONGO_SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 4;
}

my $db = $conn->get_database('test_database');
$db->drop;

my $coll = $db->get_collection('test_collection');

my $now = DateTime->now;
$coll->insert( { date => $now } );

my $date1 = $coll->find_one->{date};
isa_ok $date1, 'DateTime';

is $date1->epoch, $now->epoch;

$conn->dt_type( undef );

my $date2 = $coll->find_one->{date};
ok( not ref $date2 );
is $date2, $now->epoch;
