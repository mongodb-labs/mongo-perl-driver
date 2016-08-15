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
use Test::More;
use Test::Fatal;
use Test::Deep;
use Tie::IxHash;
use boolean;

use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;
use MongoDB::_Constants;
use MongoDB::Error;
use MongoDB::WriteConcern;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type/;

skip_unless_mongod();

$ENV{PERL_MONGO_NO_DEP_WARNINGS} = 1;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $db_name = $testdb->name;
my $server_version = server_version($conn);
my $server_type = server_type($conn);

# make sure database is created; otherwise eval tests fail on sharded 2.4
# and 3.2
$testdb->coll("test")->insert({});

subtest 'getlasterror' => sub {
    plan skip_all => "MongoDB 1.5+ needed"
        unless $server_version >= v1.5.0;

    $testdb->run_command([ismaster => 1]);
    my $result = $testdb->last_error({fsync => 1});
    is($result->{ok}, 1);
    is($result->{err}, undef);

    $result = $testdb->last_error;
    is($result->{ok}, 1, 'last_error: ok');
    is($result->{err}, undef, 'last_error: err');

    # mongos never returns 'n'
    is($result->{n}, $server_type eq 'Mongos' ? undef : 0, 'last_error: n');
};

subtest "eval (deprecated)" => sub {
    plan skip_all => "eval not available under auth"
        if $conn->password;
    my $hello = $testdb->eval('function(x) { return "hello, "+x; }', ["world"]);
    is('hello, world', $hello, 'db eval');

    like(
        exception { $testdb->eval('function(x) { xreturn "hello, "+x; }', ["world"]) },
        qr/SyntaxError/,
        'js err'
    );
};

done_testing;
