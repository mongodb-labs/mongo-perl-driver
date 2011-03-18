use strict;
use warnings;
use Test::More;

use MongoDB;

my $port = 27272;
my $started = start_mongod($port);

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::Connection->new(
        host => $host,
        port => $port,
        auto_reconnect => 1
    );
};

($@ || !$started)
    ? plan skip_all => ($@ || "couldn't start mongod")
    : plan tests => 4;

isa_ok( $conn,'MongoDB::Connection' );

my $db = $conn->test_database;
isa_ok($db, 'MongoDB::Database', 'get_database, initial connection');

my $cl = $db->foo;
isa_ok($cl, 'MongoDB::Collection', 'foo collection, initial connection');

my $id = $cl->insert({ pre => 'stop' });
note $id;

stop_mongod();
start_mongod($port);

$cl = $db->foo;
isa_ok($db, 'MongoDB::Collection', 'bar collection, auto reconnected');

$id = $cl->insert({ post => 'reconnect' });
note $id;

stop_mongod();

sub start_mongod {

    my ($port) = @_;

    my $cmd = "mongod --dbpath . --port $port --fork --logpath mongod.log";
    system $cmd;
    sleep 3;
    return !$?;
}

sub stop_mongod {

    open(my $fh,'<','mongod.lock') || fail "couldn't open mongod.lock file";
    my $pid = <$fh>;
    system "kill $pid";
    sleep 3;
}
