use strict;
use warnings;
use Test::More tests => 9;
use Test::Exception;

use Mongo;

throws_ok {
    Mongo::Connection->new(host => 'localhost', port => 1);
} qr/couldn't connect to server/, 'exception on connection failure';

my $conn;
lives_ok {
    $conn = Mongo::Connection->new;
} 'successful connection';
isa_ok($conn, 'Mongo::Connection');

is($conn->host, 'localhost', 'host default value');
is($conn->port, '27017',     'port default value');

my $db = $conn->get_database('test_database');
isa_ok($db, 'Mongo::Database', 'get_database');

$db->get_collection('test_collection')->insert({ foo => 42 });

ok((grep { $_ eq 'test_database' } $conn->database_names), 'database_names');

lives_ok {
    $db->drop;
} 'drop database';

ok(!(grep { $_ eq 'test_database' } $conn->database_names), 'database got dropped');
