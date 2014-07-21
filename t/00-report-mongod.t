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

use lib "t/lib";
use MongoDBTest qw/build_client server_version server_type/;

my $conn = build_client();
my $server_version = server_version($conn);
my $server_type = server_type($conn);

diag "Checking MongoDB test environment";

diag "\$ENV{MONGOD}=".$ENV{MONGOD} if $ENV{MONGOD};

diag "MongoDB version $server_version ($server_type)";

pass("checked MongoDB test environment");

done_testing;
