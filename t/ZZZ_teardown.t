use strict;
use warnings;
use Test::More;

use FindBin;
use lib $FindBin::Bin;
use MongoDB_TestUtils;

plan tests => 2;

# if mongod isn't running then [try to] start it up, otherwise
# all tests get skipped, which isn't much use
for my $port ( 27017,port() ) {

    mconnect($port)
        ? ok( stop_mongod(),"stopped our mongod on port $port" )
        : pass "we didn't start mongod on port $port";
}
