use strict;
use warnings;
use Test::More;

use FindBin;
use lib $FindBin::Bin;
use MongoDB_TestUtils;

plan tests => 2;

# check we can start/stop as some tests will need to do this
ok( start_mongod(),'we can start mongod' );
ok( stop_mongod(),'we can stop mongod' );

# if mongod isn't running then [try to] start it up, otherwise
# all tests get skipped, which isn't much use
mconnect() || do {

    start_mongod(27017) || BAIL_OUT "mongod not running and couldn't be started";
};
