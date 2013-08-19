#
#  Copyright 2009-2013 10gen, Inc.
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
use Test::More;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;
use DateTime;
use DateTime::Tiny;

use lib "t/lib";
use MongoDBTest '$conn';

plan skip_all => "connecting to default host/port won't work with a remote db" if exists $ENV{MONGOD};

plan tests => 1;

# test that Connection delegates constructor params to MongoClient correctly
my $conn2 = MongoDB::Connection->new( host => '127.0.0.1' );

is ( $conn2->host, '127.0.0.1' );
