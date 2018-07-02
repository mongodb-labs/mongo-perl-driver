#  Copyright 2018 - present MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

use strict;
use warnings;
use Test::More 0.96;

use MongoDB;
use MongoDB::Code;
use MongoDB::Timestamp;
use JSON::MaybeXS;
use Test::Fatal;
use boolean;
use BSON::Types ':all';

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db
    server_type server_version/;

skip_unless_mongod();

$ENV{PERL_MONGO_NO_DEP_WARNINGS} = 1;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_type = server_type($conn);
my $server_version = server_version($conn);

my $coll = $testdb->get_collection('y');
$coll->drop;

my $id = bson_oid();
isa_ok($id, 'BSON::OID');
is($id."", $id->value);

# OIDs created in time-ascending order
{
    my @ids;
    for (0..9) {
        push @ids, bson_oid();
    }
    for (0..8) {
        ok($ids[$_] < $ids[$_+1]);
    }
}

# creating ids from an existing value
{
    my $value = "012345678901234567890abc";
    my $id = bson_oid($value);
    is($id->value, $value);

    my $id_orig = bson_oid();
    foreach my $args (
        $id_orig->value,
        uc( $id_orig->value ),
        $id_orig->value,
        $id_orig,
    ) {
        my $id_copy = bson_oid($args);
        is($id_orig->value, $id_copy->value);
    }
}

# invalid ids from an existing value
{
    my $value = "506b37b1a7e2037c1f0004";
    like(
        exception { bson_oid($value) },
        qr/must be 12 packed bytes or 24 bytes of hex/i,
        "Invalid OID throws exception"
    );
}

#regexes
{
    $coll->insert_one({'x' => 'FRED', 'y' => 1});
    $coll->insert_one({'x' => 'bob'});
    $coll->insert_one({'x' => 'fRed', 'y' => 2});

    my $freds = $coll->query({'x' => qr/fred/i})->sort({'y' => 1});

    is($freds->next->{'x'}, 'FRED', 'case insensitive');
    is($freds->next->{'x'}, 'fRed', 'case insensitive');
    ok(!$freds->has_next, 'bob doesn\'t match');

    my $fred = $coll->find_one({'x' => qr/^F/});
    is($fred->{'x'}, 'FRED', 'starts with');

    # saving/getting regexes
    $coll->drop;
    $coll->insert_one({"r" => qr/foo/i});
    my $obj = $coll->find_one;
    my $qr = $obj->{r}->try_compile;
    like("foo", $qr, 'matches');
    like("FOO", $qr, "flag i works");
    unlike("bar", $qr, 'not a match');
}

# date
{
    $coll->drop;

    my $now = bson_time();
    $coll->insert_one({'date' => $now});
    my $doc = $coll->find_one;
    my $date = $doc->{'date'};
    is($date->epoch, $now->epoch);

    my $past = bson_time(1234567890);
    $coll->insert_one({'date' => $past});
    $doc = $coll->find_one({'date' => $past});
    $date = $doc->{'date'};
    is($date->epoch, $past->epoch);
}

# minkey/maxkey
{
    $coll->drop;

    my $min = bless {}, "MongoDB::MinKey";
    my $max = bless {}, "MongoDB::MaxKey";

    $coll->insert_one({min => $min, max => $max});
    my $x = $coll->find_one;

    isa_ok($x->{min}, 'BSON::MinKey');
    isa_ok($x->{max}, 'BSON::MaxKey');
}

# tie::ixhash
{
    $coll->drop;

    my %test;
    tie %test, 'Tie::IxHash';
    $test{one} = "on";
    $test{two} = 2;

    ok( $coll->insert_one(\%test), "inserted IxHash") ;

    my $doc = $coll->find_one;
    is($doc->{'one'}, 'on', "field one");
    is($doc->{'two'}, 2, "field two");
}

# binary
{
    $coll->drop;

    my $invalid = "\xFE";
    ok( $coll->insert_one({"bin" => \$invalid}), "inserted binary data" );

    my $one = $coll->find_one;
    isa_ok($one->{bin}, "BSON::Bytes", "binary data");
    is($one->{'bin'}, "\xFE", "read binary data");
}

# code
{
    $coll->drop();

    my $str = "function() { return 5; }";
    my $code = MongoDB::Code->new("code" => $str);
    my $scope = $code->scope;
    is(keys %$scope, 0);

    $coll->insert_one({"code" => $code});
    my $ret = $coll->find_one;
    my $ret_code = $ret->{code};
    $scope = $ret_code->scope;
    is(keys %$scope, 0);
    is($ret_code->code, $str);

    $str  = "function() { return name; }";
    $code = MongoDB::Code->new(
        "code"  => $str,
        "scope" => { "name" => "Fred" }
    );

    $coll->drop;

    $coll->insert_one({"x" => "foo", "y" => $code, "z" => 1});
    my $x = $coll->find_one;
    is($x->{x}, "foo");
    is($x->{y}->code, $str);
    is($x->{y}->scope->{"name"}, "Fred");
    is($x->{z}, 1);

    $coll->drop;
}

SKIP: {
    use Config;
    skip "Skipping 64 bit native SV", 1
        if ( !$Config{use64bitint} );

    $coll->update_one({ x => 1 }, { '$inc' => { 'y' => 19401194714 } }, { 'upsert' => 1 });
    my $result = $coll->find_one;
    is($result->{'y'},19401194714,'64 bit ints without Math::BigInt');
}

# oid json
{
    my $doc = {"foo" => bson_oid()};

    my $j = JSON->new;
    $j->allow_blessed;
    $j->convert_blessed;

    local $ENV{BSON_EXTJSON} = 1;
    my $json = $j->encode($doc);
    is($json, '{"foo":{"$oid":"'.$doc->{'foo'}->value.'"}}');
}

# timestamp
{
    $coll->drop;

    my $t = MongoDB::Timestamp->new("sec" => 12345678, "inc" => 9876543);
    $coll->insert_one({"ts" => $t});

    my $x = $coll->find_one;

    is($x->{'ts'}->sec, $t->sec);
    is($x->{'ts'}->inc, $t->inc);
}

# boolean objects
{
    $coll->drop;

    $coll->insert_one({"x" => boolean::true, "y" => boolean::false});
    my $x = $coll->find_one;

    is( ref $x->{x}, 'boolean', "roundtrip boolean field x");
    is( ref $x->{y}, 'boolean', "roundtrip boolean field y");
    ok( $x->{x}, "x is true");
    ok( ! $x->{y}, "y is false");
}

# unrecognized obj
{
    eval {
        $coll->insert_one({"x" => $coll});
    };

    like($@, qr/type \(MongoDB::Collection\) unhandled|can't encode value of type 'MongoDB::Collection'/, "can't insert a non-recognized obj");
}

done_testing;
