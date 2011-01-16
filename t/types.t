use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use MongoDB::OID;
use MongoDB::Code;
use MongoDB::Timestamp;
use DateTime;
use JSON;

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
    plan tests => 56;
}

my $db = $conn->get_database('x');
my $coll = $db->get_collection('y');

$coll->drop;

my $id = MongoDB::OID->new;
isa_ok($id, 'MongoDB::OID');
is($id."", $id->value);

# OIDs created in time-ascending order
{
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
}

# creating ids from an existing value
{
    my $value = "012345678901234567890123";
    my $id = MongoDB::OID->new(value => $value);
    is($id->value, $value);

    my $id_orig = MongoDB::OID->new;
    my $id_copy = MongoDB::OID->new(value => $id_orig->value);
    is($id_orig->value, $id_copy->value);
}

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

my $now = DateTime->now;

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

# tie::ixhash
{
    $coll->remove;

    my %test;
    tie %test, 'Tie::IxHash'; 
    $test{one} = "on"; 
    $test{two} = 2; 
    
    $coll->insert(\%test);

    my $doc = $coll->find_one;
    is($doc->{'one'}, 'on');
    is($doc->{'two'}, 2);
}

# binary
{
    $coll->remove;

    my $invalid = "\xFE";
    $coll->insert({"bin" => \$invalid});

    my $one = $coll->find_one;
    is($one->{'bin'}, "\xFE");
}

# 64-bit ints
{
    use bigint;
    $coll->remove;

    my $x = 2 ** 34;
    $coll->save({x => $x});
    my $result = $coll->find_one;

    is($result->{'x'}, 17179869184);

    $coll->remove;

    $x = (2 ** 34) * -1;
    $coll->save({x => $x});
    $result = $coll->find_one;

    is($result->{'x'}, -17179869184);

    $coll->remove;

    $coll->save({x => 2712631400});
    $result = $coll->find_one;
    is($result->{'x'}, 2712631400);

    eval {
        my $ok = $coll->save({x => 9834590149023841902384137418571984503});
    };

    ok($@ =~ m/BigInt is too large/);

    $coll->remove;
}

# code
{
    my $str = "function() { return 5; }";
    my $code = MongoDB::Code->new("code" => $str);
    my $scope = $code->scope;
    is(keys %$scope, 0);

    $coll->insert({"code" => $code});
    my $ret = $coll->find_one;
    my $ret_code = $ret->{code};
    $scope = $ret_code->scope;
    is(keys %$scope, 0);
    is($ret_code->code, $str);

    my $x = $db->eval($code);
    is($x, 5);

    $str = "function() { return name; }";
    $code = MongoDB::Code->new("code" => $str,
                               "scope" => {"name" => "Fred"});
    $x = $db->eval($code);
    is($x, "Fred");

    $coll->remove;

    $coll->insert({"x" => "foo", "y" => $code, "z" => 1});
    $x = $coll->find_one;
    is($x->{x}, "foo");
    is($x->{y}->code, $str);
    is($x->{y}->scope->{"name"}, "Fred");
    is($x->{z}, 1);

    $coll->remove;
}

SKIP: {
    use Config;
    skip "Skipping 64 bit native SV", 1
        if ( !$Config{use64bitint} );

    $coll->update({ x => 1 }, { '$inc' => { 'y' => 19401194714 } }, { 'upsert' => 1 });
    my $result = $coll->find_one;
    is($result->{'y'},19401194714,'64 bit ints without Math::BigInt');
}

# oid json
{
    my $doc = {"foo" => MongoDB::OID->new};

    my $j = JSON->new;
    $j->allow_blessed;
    $j->convert_blessed;

    my $json = $j->encode($doc);
    is($json, '{"foo":{"$oid":"'.$doc->{'foo'}->value.'"}}');
}

# timestamp
{
    $coll->drop;

    my $t = MongoDB::Timestamp->new("sec" => 12345678, "inc" => 9876543);
    $coll->insert({"ts" => $t});

    my $x = $coll->find_one;

    is($x->{'ts'}->sec, $t->sec);
    is($x->{'ts'}->inc, $t->inc);
}

# use_boolean
{
    $coll->drop;

    $MongoDB::BSON::use_boolean = 0;

    $coll->insert({"x" => boolean::true, "y" => boolean::false});
    my $x = $coll->find_one;

    isa_ok($x->{x}, 'SCALAR');
    isa_ok($x->{y}, 'SCALAR');
    is($x->{x}, 1);
    is($x->{y}, 0);

    $MongoDB::BSON::use_boolean = 1;

    $x = $coll->find_one;

    isa_ok($x->{x}, 'boolean');
    isa_ok($x->{y}, 'boolean');
    is($x->{x}, boolean::true);
    is($x->{y}, boolean::false);
}

# check blessed obj + attribute
{
    my $coll = $db->get_collection('test_collection');
    $coll->drop;
	# blessed hash
	my $foo = bless { name=>'foo' }, 'Person';
	$foo->{baz} = bless { name=>'boo', }, 'Something';
	$coll->insert( $foo );
	my $doc = $coll->find_one;
	is( $doc->{baz}->{name}, 'boo', 'blessed hash-refs ok' );
	$coll->drop;

	# circularity
	$foo->{circ} = $foo;
    eval {
        $coll->insert( $foo );
    };

    ok($@ =~ m/circular ref/, "can't insert a circular ref obj: $@");
}


END {
    if ($db) {
        $db->drop;
    }
}
