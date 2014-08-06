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
use Test::More 0.88;

use MongoDB;
use MongoDB::OID;
use boolean;
use DateTime;
use Data::Types qw(:float);
use Encode;
use Tie::IxHash;
use Test::Fatal;
use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB::BSON::Binary;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $testdb = get_test_db(build_client());

my $c = $testdb->get_collection('bar');

# relloc
{
    $c->drop;

    my $long_str = "y" x 8184;
    $c->insert({'text' => $long_str});
    my $result = $c->find_one;
    is($result->{'text'}, $long_str, 'realloc');
}

# id realloc
{
    $c->drop;

    my $med_str = "z" x 4014;
    $c->insert({'text' => $med_str, 'id2' => MongoDB::OID->new});
    my $result = $c->find_one;
    is($result->{'text'}, $med_str, 'id realloc');
}

{
    $c->drop;

    my $id = $c->insert({"n" => undef,
                "l" => 234234124,
                "d" => 23.23451452,
                "b" => true,
                "a" => {"foo" => "bar",
                        "n" => undef,
                        "x" => MongoDB::OID->new("49b6d9fb17330414a0c63102")},
                "d2" => DateTime->from_epoch(epoch => 1271079861),
                "regex" => qr/xtz/,
                "_id" => MongoDB::OID->new("49b6d9fb17330414a0c63101"),
                "string" => "string"});

    my $obj = $c->find_one;

    is($obj->{'n'}, undef);
    is($obj->{'l'}, 234234124);
    is($obj->{'d'}, 23.23451452);
    is($obj->{'b'}, true);
    is($obj->{'a'}->{'foo'}, 'bar');
    is($obj->{'a'}->{'n'}, undef);
    isa_ok($obj->{'a'}->{'x'}, 'MongoDB::OID');
    isa_ok($obj->{'d2'}, 'DateTime');
    is($obj->{'d2'}->epoch, 1271079861);
    ok($obj->{'regex'});
    isa_ok($obj->{'_id'}, 'MongoDB::OID');
    is($obj->{'_id'}, $id);
    is($obj->{'string'}, 'string');
}

{
    local $MongoDB::BSON::char = "=";
    $c->drop;
    $c->update({x => 1}, {"=inc" => {x => 1}}, {upsert => true});

    my $up = $c->find_one;
    is($up->{x}, 2);
}

{
    local $MongoDB::BSON::char = ":";
    $c->drop;
    $c->batch_insert([{x => 1}, {x => 2}, {x => 3}, {x => 4}, {x => 5}]);
    my $cursor = $c->query({x => {":gt" => 2, ":lte" => 4}})->sort({x => 1});

    my $result = $cursor->next;
    is($result->{x}, 3);
    $result = $cursor->next;
    is($result->{x}, 4);
    ok(!$cursor->has_next);
}

# utf8
{
    $c->drop;

    # latin1
    $c->insert({char => "\xFE"});
    my $x =$c->find_one;
    is($x->{char}, "\xFE");

    $c->remove;

    # non-latin1
    my $valid = "\x{8D4B}\x{8BD5}";
    $c->insert({char => $valid});
    $x = $c->find_one;

    # make sure it's being returned as a utf8 string
    ok(utf8::is_utf8($x->{char}));
    is(length $x->{char}, 2);
}

subtest "bad UTF8" => sub {

    my @bad = (
        "\xC0\x80"            , # Non-shortest form representation of U+0000
        "\xC0\xAF"            , # Non-shortest form representation of U+002F
        "\xE0\x80\x80"        , # Non-shortest form representation of U+0000
        "\xF0\x80\x80\x80"    , # Non-shortest form representation of U+0000
        "\xE0\x83\xBF"        , # Non-shortest form representation of U+00FF
        "\xF0\x80\x83\xBF"    , # Non-shortest form representation of U+00FF
        "\xF0\x80\xA3\x80"    , # Non-shortest form representation of U+08C0
    );

    for my $bad_utf8 ( @bad ) {
    # invalid should throw
        my $label = "0x" . unpack("H*", $bad_utf8);
        Encode::_utf8_on($bad_utf8); # force on internal UTF8 flag
        like(
            exception { $c->insert({char => $bad_utf8}) },
            qr/Invalid UTF-8 detected while encoding/,
            "invalid UTF-8 throws an error inserting $label"
        );
    }

};

# undefined
{
    my $err = $testdb->last_error();
    ok(!$err->{err}, "undef");
    $err->{err} = "foo";
    is($err->{err}, "foo", "assign to undef");
}

# circular references
{
    my $q = {};
    $q->{'q'} = $q;

    eval {
        $c->insert($q);
    };

    ok($@ =~ /circular ref/);

    my %test;
    tie %test, 'Tie::IxHash';
    $test{t} = \%test;

    eval {
        $c->insert(\%test);
    };

    ok($@ =~ /circular ref/);

    my $tie = Tie::IxHash->new;
    $tie->Push("t" => $tie);

    eval {
        $c->insert($tie);
    };

    ok($@ =~ /circular ref/);
}

# no . in key names
{
    eval {
        $c->insert({"x.y" => "foo"});
    };
    like($@, qr/documents for storage cannot contain/);

    eval {
        $c->insert({"x.y" => "foo", "bar" => "baz"});
    };
    like($@, qr/documents for storage cannot contain/);

    eval {
        $c->insert({"bar" => "baz", "x.y" => "foo"});
    };
    like($@, qr/documents for storage cannot contain/);

    eval {
        $c->insert({"bar" => {"x.y" => "foo"}});
    };
    like($@, qr/documents for storage cannot contain/);

    eval {
        $c->batch_insert([{"x" => "foo"}, {"x.y" => "foo"}, {"y" => "foo"}]);
    };
    like($@, qr/documents for storage cannot contain/);

    eval {
        $c->batch_insert([{"x" => "foo"}, {"foo" => ["x", {"x.y" => "foo"}]}, {"y" => "foo"}]);
    };
    like($@, qr/documents for storage cannot contain/);
}

# empty key name
{
    eval {
        $c->insert({"" => "foo"});
    };
    ok($@ =~ /empty key name/);
}


# moose numbers
package Person;
use Moose;
has 'name' => ( is=>'rw', isa=>'Str' );
has 'age'  => ( is=>'rw', isa=>'Int' );
has 'size' => ( is=>'rw', isa=>'Num' );

package main;
{
    $c->drop;

    my $p = Person->new( name=>'jay', age=>22 );
    $c->save($p);

    my $person = $c->find_one;
    ok(is_float($person->{'age'}));
}

# warn on floating timezone
{
    my $warned = 0;
    local $SIG{__WARN__} = sub { if ($_[0] =~ /floating/) { $warned = 1; } else { warn(@_); } };
    my $date = DateTime->new(year => 2010, time_zone => "floating");
    $c->insert({"date" => $date});
    is($warned, 1, "warn on floating timezone");
}

# half-conversion to int type
{
    $c->drop;

    my $var = 'zzz';
    # don't actually change it to an int, but add pIOK flag
    { no warnings 'numeric';
    $var = int($var) if (int($var) eq $var);
    }

    $c->insert({'key' => $var});
    my $v = $c->find_one;

    # make sure it was saved as string
    is($v->{'key'}, 'zzz');
}

# store a scalar with magic that's both a float and int (PVMG w/pIOK set)
{
    $c->drop;

    # PVMG (NV is 11.5)
    my $size = Person->new( size => 11.5 )->size;

    # add pIOK flag (IV is 11)
    { no warnings 'void';
    int($size);
    }

    $c->insert({'key' => $size});
    my $v = $c->find_one;

    # make sure it was saved as float
    is(($v->{'key'}), $size);
}

# make sure _ids aren't double freed
{
    $c->drop;

    my $insert1 = ['_id' => 1];
    my $insert2 = Tie::IxHash->new('_id' => 2);

    my $id = $c->insert($insert1, {safe => 1});
    is($id, 1);

    $id = $c->insert($insert2, {safe => 1});
    is($id, 2);
}

# aggressively convert numbers
{
    $MongoDB::BSON::looks_like_number = 1;

    $c->drop;

    $c->insert({num => "4"});
    $c->insert({num => "5"});
    $c->insert({num => "6"});

    $c->insert({num => 4});
    $c->insert({num => 5});
    $c->insert({num => 6});

    is($c->count({num => {'$gt' => 4}}), 4);
    is($c->count({num => {'$gte' => "5"}}), 4);
    is($c->count({num => {'$gte' => "4.1"}}), 4);

    $MongoDB::BSON::looks_like_number = 0;
}

# MongoDB::BSON::String type
{
    $MongoDB::BSON::looks_like_number = 1;

    $c->drop;

    my $num = "001";

    $c->insert({num => $num}, {safe => 1});
    $c->insert({num => bless(\$num, "MongoDB::BSON::String")}, {safe => 1});

    $MongoDB::BSON::looks_like_number = 0;

    is($c->count({num => 1}), 1);
    is($c->count({num => "001"}), 1);
    is($c->count, 2);
}

# MongoDB::BSON::Binary type
{
    $c->drop;

    local $MongoDB::BSON::use_binary = 0;

    my $str = "foo";
    my $bin = {bindata => [
                   \$str,
                   MongoDB::BSON::Binary->new(data => $str),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_GENERIC),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_FUNCTION),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_GENERIC_DEPRECATED),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_UUID_DEPRECATED),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_UUID),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_MD5),
                   MongoDB::BSON::Binary->new(data => $str, subtype => MongoDB::BSON::Binary->SUBTYPE_USER_DEFINED)]};

    $c->insert($bin, {safe => 1});

    my $doc = $c->find_one;

    my $data = $doc->{'bindata'};
    foreach (@$data) {
        is($_, "foo");
    }

    $MongoDB::BSON::use_binary = 1;

    $doc = $c->find_one;

    $data = $doc->{'bindata'};
    my @arr = @$data;

    is($arr[0]->subtype, MongoDB::BSON::Binary->SUBTYPE_GENERIC);
    is($arr[0]->data, $str);

    for (my $i=1; $i<=$#arr; $i++ ) {
        is($arr[$i]->subtype, $bin->{'bindata'}->[$i]->subtype);
        is($arr[$i]->data, $bin->{'bindata'}->[$i]->data);
    }
}

# Checking hash key unicode support
{
    use utf8;
    $c->drop;
    
    my $testkey = 'юникод';
    my $hash = { $testkey => 1 };

    my $oid;
    eval { $oid = $c->insert( $hash, {safe => 1}); };
    is ( $@, '' );
    my $obj = $c->find_one( { _id => $oid } );
    is ( $obj->{$testkey}, 1 );
}

done_testing;
