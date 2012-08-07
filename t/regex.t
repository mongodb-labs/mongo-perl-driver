use strict;
use warnings;
use Test::More;
use Test::Exception;
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;

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

warning_like { 
    $coll->insert( { name => 'foo', test_regex => qr/foo/iu } )
} qr/unsupported regex flag u/, 'unsupported flag warning';

my ( $doc ) = $coll->find_one( { name => 'foo' } );
is $doc->{test_regex}, qr/foo/i;
