#  Copyright 2018 - present MongoDB, Inc.
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

use strict;
use warnings;
use utf8;
use Test::More 0.96;
use Test::Fatal;

use MongoDB;

plan skip_all => "\$ENV{MONGOD} not set"
  unless $ENV{MONGOD};

my $mc = MongoDB->connect(
    $ENV{MONGOD},
    {
        server_selection_timeout_ms => 10000,
        server_selection_try_once   => 0,
    }
);

diag "\nConnecting to " . $mc->_uri;

is( exception { $mc->db("admin")->run_command( [ ismaster => 1 ] ) },
    undef, "ismaster" );
is( exception { $mc->ns("test.tst")->find_one() }, undef, "find_one" );

done_testing;
