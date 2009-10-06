use strict;
use warnings;
use Test::More tests => 12;
use Test::Exception;

use MongoDB;
use MongoDB::OID;
use DateTime;

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

# saving/getting regexes
$coll->drop;
$coll->insert({"r" => qr/foo/i});
my $obj = $coll->find_one;
ok("foo" =~ $obj->{'r'}, 'matches');

SKIP: {
    skip "regex flags don't work yet with perl 5.8", 1 if $] =~ /5\.008/;
    ok("FOO" =~ $obj->{'r'}, 'this won\'t pass with Perl 5.8');
}

ok(!("bar" =~ $obj->{'r'}), 'not a match');


# date
$coll->drop;

my $now = DateTime->now;

$coll->insert({'date' => $now});
my $date = $coll->find_one;

is($date->{'date'}->epoch, $now->epoch);
is($date->{'date'}->day_of_week, $now->day_of_week);

my $past = DateTime->from_epoch('epoch' => 1234567890);

$coll->insert({'date' => $past});
$date = $coll->find_one({'date' => $past});

is($date->{'date'}->epoch, 1234567890);

