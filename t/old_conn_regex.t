use strict;
use warnings;
use Test::More;
use Test::Exception;
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;

if ( $^V lt 5.14.0 ) { 
    plan skip_all => 'we need perl 5.14 for regex tests';
}

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
    plan tests => 2;
}

my $db = $conn->get_database('test_database');
$db->drop;

my $coll = $db->get_collection('test_collection');

my $test_regex = eval 'qr/foo/iu';    # eval regex to prevent compile failure on pre-5.14
warning_like { 
    $coll->insert( { name => 'foo', test_regex => $test_regex } )
} qr{unsupported regex flag /u}, 'unsupported flag warning';


my ( $doc ) = $coll->find_one( { name => 'foo' } );
is $doc->{test_regex}, qr/foo/i;
