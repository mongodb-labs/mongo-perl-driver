#
#  Copyright 2009-2013 MongoDB, Inc.
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
#

use strict;
use warnings;
use Test::More 0.96;

use MongoDB;
use MongoDB::OID;
use MongoDB::Code;
use MongoDB::Timestamp;
use DateTime;
use JSON::MaybeXS;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client();
my $testdb = get_test_db($conn);

my $coll = $testdb->get_collection('y');
$coll->drop;

my $id = MongoDB::OID->new;
isa_ok($id, 'MongoDB::OID');
is($id."", $id->value);

# OIDs created in time-ascending order
{
    my $ids = [];
    for (0..9) {
        push @$ids, new MongoDB::OID;
        select undef, undef, undef, 0.1;  # Sleep 0.1 seconds
    }
    for (0..8) {
        ok((@$ids[$_]."") lt (@$ids[$_+1].""));
    }
    
    my $now = DateTime->now;
    $id = MongoDB::OID->new;
    
    ok($id->get_time >= $now->epoch, "OID time >= epoch" );
}

# creating ids from an existing value
{
    my $value = "012345678901234567890123";
    my $id = MongoDB::OID->new(value => $value);
    is($id->value, $value);

    my $id_orig = MongoDB::OID->new;
    foreach my $args (
        [value => $id_orig->value],
        [$id_orig->value],
        [$id_orig],
    ) {
        my $id_copy = MongoDB::OID->new(@{$args});
        is($id_orig->value, $id_copy->value);
    }
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
    ok("foo" =~ $obj->{'r'}, 'matches');

    SKIP: {
        skip "regex flags don't work yet with perl 5.8", 1 if $] =~ /5\.008/;
        ok("FOO" =~ $obj->{'r'}, 'this won\'t pass with Perl 5.8');
    }

    ok(!("bar" =~ $obj->{'r'}), 'not a match');
}

# date
{
    $coll->drop;

    my $now = DateTime->now;

    $coll->insert_one({'date' => $now});
    my $date = $coll->find_one;

    is($date->{'date'}->epoch, $now->epoch);
    is($date->{'date'}->day_of_week, $now->day_of_week);

    my $past = DateTime->from_epoch('epoch' => 1234567890);

    $coll->insert_one({'date' => $past});
    $date = $coll->find_one({'date' => $past});

    is($date->{'date'}->epoch, 1234567890);
}

# minkey/maxkey
{
    $coll->drop;

    my $min = bless {}, "MongoDB::MinKey";
    my $max = bless {}, "MongoDB::MaxKey";

    $coll->insert_one({min => $min, max => $max});
    my $x = $coll->find_one;

    isa_ok($x->{min}, 'MongoDB::MinKey');
    isa_ok($x->{max}, 'MongoDB::MaxKey');
}

# tie::ixhash
{
    $coll->remove;

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
    $coll->remove;

    my $invalid = "\xFE";
    ok( $coll->insert_one({"bin" => \$invalid}), "inserted binary data" );

    my $one = $coll->find_one;
    is($one->{'bin'}, "\xFE", "read binary data");
}

# 64-bit ints
{
    use bigint;
    $coll->remove;

    my $x = 2 ** 34;
    $coll->save({x => $x});
    my $result = $coll->find_one;

    is($result->{'x'}, 17179869184)
        or diag explain $result;

    $coll->remove;

    $x = (2 ** 34) * -1;
    $coll->save({x => $x});
    $result = $coll->find_one;

    is($result->{'x'}, -17179869184)
        or diag explain $result;

    $coll->remove;

    $coll->save({x => 2712631400});
    $result = $coll->find_one;
    is($result->{'x'}, 2712631400)
        or diag explain $result;

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

    $coll->insert_one({"code" => $code});
    my $ret = $coll->find_one;
    my $ret_code = $ret->{code};
    $scope = $ret_code->scope;
    is(keys %$scope, 0);
    is($ret_code->code, $str);

    my $x;

    if ( ! $conn->password ) {
        $x = $testdb->eval($code);
        is($x, 5);
    }

    $str = "function() { return name; }";
    $code = MongoDB::Code->new("code" => $str,
                               "scope" => {"name" => "Fred"});
    if ( ! $conn->password ) {
        $x = $testdb->eval($code);
        is($x, "Fred");
    }

    $coll->remove;

    $coll->insert_one({"x" => "foo", "y" => $code, "z" => 1});
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

    $coll->update_one({ x => 1 }, { '$inc' => { 'y' => 19401194714 } }, { 'upsert' => 1 });
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
    $coll->insert_one({"ts" => $t});

    my $x = $coll->find_one;

    is($x->{'ts'}->sec, $t->sec);
    is($x->{'ts'}->inc, $t->inc);
}

# use_boolean
{
    $coll->drop;

    $MongoDB::BSON::use_boolean = 0;

    $coll->insert_one({"x" => boolean::true, "y" => boolean::false});
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

# unrecognized obj
{
    eval {
        $coll->insert_one({"x" => $coll});
    };

    ok($@ =~ m/type \(MongoDB::Collection\) unhandled/, "can't insert a non-recognized obj");
}


# forcing types
{
    $coll->drop;

    my $x = 1.0;
    my ($double_type, $int_type) = ({x => {'$type' => 1}},
                                    {'$or' => [{x => {'$type' => 16}},
                                               {x => {'$type' => 18}}]});

    MongoDB::force_double($x);
    $coll->insert_one({x => $x});
    my $result = $coll->find_one($double_type);
    is($result->{x}, 1);
    $result = $coll->find_one($int_type);
    is($result, undef);
    $coll->remove({});

    MongoDB::force_int($x);
    $coll->insert_one({x => $x});
    $result = $coll->find_one($double_type);
    is($result, undef);
    $result = $coll->find_one($int_type);
    is($result->{x}, 1);
}

done_testing;
