use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;

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
    plan tests => 9;
}

throws_ok {
    MongoDB::Connection->new(host => 'localhost', port => 1);
} qr/couldn't connect to server/, 'exception on connection failure';

SKIP: {
    skip "connecting to default host/port won't work with a remote db", 4 if exists $ENV{MONGOD};

    lives_ok {
        $conn = MongoDB::Connection->new;
    } 'successful connection';
    isa_ok($conn, 'MongoDB::Connection');
    
    is($conn->host, 'localhost', 'host default value');
    is($conn->port, '27017',     'port default value');

    # just make sure a couple timeouts work
    my $to = MongoDB::Connection->new('timeout' => 1);
    $to = MongoDB::Connection->new('timeout' => 123);
    $to = MongoDB::Connection->new('timeout' => 2000000);
}

my $db = $conn->get_database('test_database');
isa_ok($db, 'MongoDB::Database', 'get_database');

$db->get_collection('test_collection')->insert({ foo => 42 });

ok((grep { $_ eq 'test_database' } $conn->database_names), 'database_names');

lives_ok {
    $db->drop;
} 'drop database';

ok(!(grep { $_ eq 'test_database' } $conn->database_names), 'database got dropped');
