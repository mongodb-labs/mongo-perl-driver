package MongoDBTest;

use strict;
use warnings;

use Exporter 'import';
use MongoDB;
use Test::More;

our @EXPORT_OK = ( '$conn' );
our $conn;

# set up connection if we can
BEGIN { 
    eval { 
        my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';
        $conn = MongoDB::MongoClient->new( host => $host, ssl => $ENV{MONGO_SSL} );
    };

    if ( $@ ) { 
        plan skip_all => $@;
        exit 0;
    }
};


# clean up any detritus from failed tests
END { 
    return unless $conn;

    $conn->get_database( 'test_database' )->drop;
};


