use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use MongoDB::OID;
use boolean;
use DateTime;
use Tie::IxHash;

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


END {
    if ($db) {
        $db->drop;
    }
}
