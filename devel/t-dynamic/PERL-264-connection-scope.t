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
use Scalar::Util qw/refaddr/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use MongoDBTest::Orchestrator;

my $orc =
  MongoDBTest::Orchestrator->new( config_file => "devel/clusters/mongod-2.6.yml" );
$orc->start;
$ENV{MONGOD} = $orc->as_uri;
diag "MONGOD: $ENV{MONGOD}";

use MongoDBTest qw/build_client/;

my $conn = build_client();

# test for PERL-264
{
    my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';
    my ($connections, $start);
    for (1..10) {
        my $conn2 = build_client();
        $connections = $conn->get_database("admin")->_try_run_command([serverStatus => 1])->{connections}{current};
        $start = $connections unless defined $start
    }
    is(abs($connections-$start) < 3, 1, 'connection dropped after scope');
}

done_testing;
