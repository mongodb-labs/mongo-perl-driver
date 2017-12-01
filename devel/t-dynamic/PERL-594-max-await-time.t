#
#  Copyright 2014 MongoDB, Inc.
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
use Regexp::Common qw /balanced/;
use Time::HiRes qw/time/;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs server_version get_capped/;

sub _test_max_await {
    my $find_options = shift || {};

    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );

    my $testdb = get_test_db($conn);
    my $server_version = server_version($conn);
    note "Server Version: $server_version";

    my $coll = get_capped($testdb);
    $coll->insert_one({x => $_}) for 1 .. 50;

    my $res =
      $coll->find( {}, { cursorType => 'tailable_await', %$find_options } )->result;
    $res->all;

    my $before = time();
    $res->next;
    my $after = time();

    ok( $after > $before, sprintf("tailable await waited %.9f seconds", $after-$before) );

    return $after-$before;
}

sub _maxTimeMS_like {
    my ( $orc, $cmd, $num ) = @_;
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my ($log) = $orc->get_server('host1')->grep_log(qr/command: $cmd/);
    # getMore includes originatingCommand, so grab args we want to check
    my ($args) = $log =~ m{command: $cmd ($RE{balanced}{-parens=>'{}'})};
    ok( $args, "found $cmd args" );
    if ( defined $num ) {
        like( $args, qr/maxTimeMS: $num/, "$cmd MaxTimeMS is $num" );
    }
    else {
        unlike( $args, qr/maxTimeMS:/, "$cmd MaxTimeMS is unset" );
    }
}

subtest "default" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-3.4.yml" );
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    my $wait = _test_max_await();
    _maxTimeMS_like($orc, 'find', undef);
    _maxTimeMS_like($orc, 'getMore', undef);
};

subtest "maxTimeMS nonzero" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-3.4.yml" );
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    my $wait = _test_max_await({maxTimeMS => 5000});
    _maxTimeMS_like($orc, 'find', 5000);
    _maxTimeMS_like($orc, 'getMore', undef);
    ok( $wait < 5, "await was not as long as maxTimeMS on find()" );
};

subtest "maxAwaitTimeMS nonzero" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-3.4.yml" );
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    my $wait = _test_max_await({maxAwaitTimeMS => 2000});
    _maxTimeMS_like($orc, 'find', undef);
    _maxTimeMS_like($orc, 'getMore', 2000);
    ok( $wait > 2, "await was longer than maxAwaitTimeMS on find()" );
};

clear_testdbs;

done_testing;

