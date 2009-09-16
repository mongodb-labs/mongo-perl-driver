use strict;
use warnings;
use Test::More tests => 6;
use Test::Exception;

use MongoDB;
use MongoDB::OID;

my $conn = MongoDB::Connection->new;
my $db = $conn->get_database('x');
my $coll = $db->get_collection('y');

$coll->drop;

my $id = MongoDB::OID->new;
isa_ok($id, 'MongoDB::OID');
is($id."", $id->value);

$coll->insert({'x' => 'FRED', 'y' => 1});
$coll->insert({'x' => 'bob'});
$coll->insert({'x' => 'fRed', 'y' => 2});

my $freds = $coll->query({'x' => qr/fred/i})->sort({'y' => 1});

is($freds->next->{'x'}, 'FRED', 'case insensitive');
is($freds->next->{'x'}, 'fRed', 'case insensitive');
ok(!$freds->has_next, 'bob doesn\'t match');

my $fred = $coll->find_one({'x' => qr/^F/});
is($fred->{'x'}, 'FRED', 'starts with');

