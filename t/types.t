use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use MongoDB::OID;
use DateTime;

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::Connection->new(host => $host);
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 24;
}

my $db = $conn->get_database('x');
my $coll = $db->get_collection('y');

$coll->drop;

my $id = MongoDB::OID->new;
isa_ok($id, 'MongoDB::OID');
is($id."", $id->value);

# OIDs created in time-ascending order
my $ids = [];
for (0..9) {
    push @$ids, new MongoDB::OID;
    sleep 1;
}
for (0..8) {
    ok((@$ids[$_]."") lt (@$ids[$_+1].""));
}

my $now = DateTime->now;
$id = MongoDB::OID->new;

is($now->epoch, $id->get_time);

#regexes

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

$now = DateTime->now;

$coll->insert({'date' => $now});
my $date = $coll->find_one;

is($date->{'date'}->epoch, $now->epoch);
is($date->{'date'}->day_of_week, $now->day_of_week);

my $past = DateTime->from_epoch('epoch' => 1234567890);

$coll->insert({'date' => $past});
$date = $coll->find_one({'date' => $past});

is($date->{'date'}->epoch, 1234567890);

# minkey/maxkey
$coll->drop;

my $min = bless {}, "MongoDB::MinKey";
my $max = bless {}, "MongoDB::MaxKey";

$coll->insert({min => $min, max => $max});
my $x = $coll->find_one;

isa_ok($x->{min}, 'MongoDB::MinKey');
isa_ok($x->{max}, 'MongoDB::MaxKey');

END {
    if ($db) {
        $db->drop;
    }
}
