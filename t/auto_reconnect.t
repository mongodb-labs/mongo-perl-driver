use strict;
use warnings;
use Test::More;

use MongoDB;

use FindBin;
use lib $FindBin::Bin;
use MongoDB_TestUtils;

my $started = restart_mongod();
my $conn    = mconnect();

($@ || !$started)
    ? plan skip_all => ($@ || "couldn't start mongod")
    : plan tests => 7;

isa_ok( $conn,'MongoDB::Connection' );

my $db = $conn->test_database;
isa_ok($db, 'MongoDB::Database', 'get_database, initial connection');

my $cl = $db->foo;
isa_ok($cl, 'MongoDB::Collection', 'foo collection, initial connection');

my $id = $cl->insert({ pre => 'stop' });
inserted_ok($cl,$id);

restart_mongod();

$id = $cl->insert({ post => 'reconnect' });
inserted_ok($cl,$id);

fail $@ if $@;

sub inserted_ok {

    my ($cl, $id) = @_;

    ok($cl->find({_id => $id}), "$id inserted (find)");
    ok($cl->find_one({_id => $id}), "$id inserted (find_one)");
}
