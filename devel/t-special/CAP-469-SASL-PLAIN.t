#
#  Copyright 2009-2013 MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

use strict;
use warnings;
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use boolean;

use MongoDB;

# REQUIRES DRIVER COMPILED WITH SASL SUPPORT
#
# See also https://wiki.mongodb.com/display/DH/Testing+Kerberos
#
# Set host, username, password in ENV vars: MONGOD, MONGOUSER, MONGOPASS

my $mc = MongoDB::MongoClient->new(
    host     => $ENV{MONGOD},
    username => $ENV{MONGOUSER},
    password => $ENV{MONGOPASS},
    sasl     => 1,
    sasl_mechanism => 'PLAIN',
);

ok( $mc, "authentication succeeded" );
is(
    exception { $mc->get_database("test")->_try_run_command([ismaster => 1]) },
    undef,
    "ismaster succeeded"
);

done_testing;
