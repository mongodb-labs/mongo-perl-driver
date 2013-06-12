use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;
use Scalar::Util 'blessed', 'reftype';

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::MongoClient->new(host => $host, ssl => $ENV{MONGO_SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 6;
}

$conn->w( -1 );
is( $conn->_w_want_safe, 0 );

$conn->w( 0 );
is( $conn->_w_want_safe, 0 );

$conn->w( 1 );
is( $conn->_w_want_safe, 1 );

$conn->w( 'all' );
is( $conn->_w_want_safe, 1 );

$conn->w( 'majority' );
is( $conn->_w_want_safe, 1 );

$conn->w( 'anything' );
is( $conn->_w_want_safe, 1 );
