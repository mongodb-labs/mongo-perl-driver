use strict;
use warnings;
use Test::More;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;
use DateTime;
use DateTime::Tiny;

use lib "t/lib";
use MongoDBTest '$conn';

plan tests => 1;

# test that Connection delegates constructor params to MongoClient correctly
my $conn2 = MongoDB::Connection->new( host => '127.0.0.1' );

is ( $conn2->host, '127.0.0.1' );
