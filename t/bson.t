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
    $conn = MongoDB::Connection->new;
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 15;
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

END {
    if ($db) {
        $db->drop;
    }
}
