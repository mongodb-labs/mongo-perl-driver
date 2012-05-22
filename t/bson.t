use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use MongoDB::OID;
use boolean;
use DateTime;
use Data::Types qw(:float);
use Tie::IxHash;
use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB::BSON::Binary;

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
    plan tests => 73;
}

my $db = $conn->get_database('foo');
my $c = $db->get_collection('bar');

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
    $MongoDB::BSON::char = "=";
    $c->drop;
    $c->update({x => 1}, {"=inc" => {x => 1}}, {upsert => true});

    my $up = $c->find_one;
    is($up->{x}, 2);
}

{
    $MongoDB::BSON::char = ":";
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

    # should convert invalid utf8 to valid
    my $invalid = "\xFE";
    $c->insert({char => $invalid});
    my $x =$c->find_one;
    # now that the utf8 flag is set, it converts it back to a single char for
    # unknown reasons
    is($x->{char}, "\xFE");

    $c->remove;

    # should be the same with valid utf8
    my $valid = "\xE6\xB5\x8B\xE8\xAF\x95";
    $c->insert({char => $valid});
    $x = $c->find_one;

    # make sure it's being returned as a utf8 string
    ok(utf8::is_utf8($x->{char}));
    is(length $x->{char}, 2);
}

# undefined
{
    my $err = $db->last_error();
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
    ok($@ =~ /inserts cannot contain/);

    eval {
        $c->insert({"x.y" => "foo", "bar" => "baz"});
    };
    ok($@ =~ /inserts cannot contain/);

    eval {
        $c->insert({"bar" => "baz", "x.y" => "foo"});
    };
    ok($@ =~ /inserts cannot contain/);

    eval {
        $c->insert({"bar" => {"x.y" => "foo"}});
    };
    ok($@ =~ /inserts cannot contain/);

    eval {
        $c->batch_insert([{"x" => "foo"}, {"x.y" => "foo"}, {"y" => "foo"}]);
    };
    ok($@ =~ /inserts cannot contain/);

    eval {
        $c->batch_insert([{"x" => "foo"}, {"foo" => ["x", {"x.y" => "foo"}]}, {"y" => "foo"}]);
    };
    ok($@ =~ /inserts cannot contain/);
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
use Any::Moose;
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
    my $date = DateTime->new(year => 2010, time_zone => "floating");
    $c->insert({"date" => $date});
}

# half-conversion to int type
{
    $c->drop;

    my $var = 'zzz';
    # don't actually change it to an int, but add pIOK flag
    $var = int($var) if (int($var) eq $var);

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
    int($size);

    $c->insert({'key' => $size});
    my $v = $c->find_one;

    # make sure it was saved as float
    is(($v->{'key'}), $size);
}

# make sure this doesn't segfault
{
    use utf8;

    eval {
        $c->insert({'_id' => 'bar', '上海' => 'ouch'});
    };
    ok($@ =~ /could not find hash value for key/, "error: ".$@);
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

    my $old = $MongoDB::BSON::use_binary;
    $MongoDB::BSON::use_binary = 0;

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

    $MongoDB::BSON::use_binary = $old;
}

END {
    if ($db) {
        $db->drop;
    }
}
