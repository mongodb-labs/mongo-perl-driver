use strict;
use warnings;
use Test::More tests => 4;
use Test::Exception;

use MongoDB;
use MongoDB::OID;

my $conn = MongoDB::Connection->new;
my $db = $conn->get_database('x');
my $coll = $db->get_collection('y');

my $id = MongoDB::OID->new;
isa_ok($id, 'MongoDB::OID');
is($id."", $id->value);

$coll->insert({'x' => 'FRED'});
my $fred = $coll->find_one({'x' => qr/fred/i});
is($fred->{'x'}, 'FRED', 'case insensitive');

$fred = $coll->find_one({'x' => qr/^F/});
is($fred->{'x'}, 'FRED', 'starts with');

