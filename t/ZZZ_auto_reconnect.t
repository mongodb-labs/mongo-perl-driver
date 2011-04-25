use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;

use FindBin;
use lib $FindBin::Bin;
use MongoDB_TestUtils;

my $started = restart_mongod();
my $conn    = mconnect();

($@ || !$started)
    ? plan skip_all => ($@ || "couldn't start mongod")
    : plan tests => 16;

isa_ok( $conn,'MongoDB::Connection' );

my $db = $conn->test_database;
isa_ok($db, 'MongoDB::Database', 'get_database, initial connection');

my $cl = $db->foo;
isa_ok($cl, 'MongoDB::Collection', 'foo collection, initial connection');

my $id = $cl->insert({ pre => 'stop' },{ safe => 1 });
inserted_ok($cl,$id);

restart_mongod();

# reconnect on insert is not "safe" so croaks
throws_ok(
    sub { $id = $cl->insert({ post => 'reconnect' },{ safe => 1 }) },
    qr/reconnected/,
    'safe insert post reconnect errors'
);

$id = $cl->insert({ post => 'reconnect' },{ safe => 1 });

# restart again to check reconnect on find isn't fatal
restart_mongod();
inserted_ok($cl,$id);

stop_mongod();

sub inserted_ok {

    my ($cl, $id) = @_;

    ok($id,"id returned from insert ($id)");

    for ( 1 .. 2 ) {

        my $res = $cl->find({_id => $id});
        ok($res,"$id (find attempt $_)");

        my $next = $res->next;
        ok($next, "$id (find->next)");
    }

    ok($cl->find_one({_id => $id}), "$id inserted (find_one)");
}
