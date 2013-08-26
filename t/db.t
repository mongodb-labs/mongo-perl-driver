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
use Test::Exception;

use MongoDB;

use lib "t/lib";
use MongoDBTest '$conn';

plan tests => 10;

my $db   = $conn->get_database('test_database');

my $result = $db->run_command({reseterror => 1});
is($result->{ok}, 1, 'reset error');

$result = $db->last_error;
is($result->{ok}, 1, 'last_error1');
is($result->{n}, 0, 'last_error2');
is($result->{err}, undef, 'last_error3');

$db->run_command({forceerror => 1});

$result = $db->last_error;
is($result->{ok}, 1, 'last_error1');
is($result->{n}, 0, 'last_error2');
is($result->{err}, 'forced error', 'last_error3');

my $hello = $db->eval('function(x) { return "hello, "+x; }', ["world"]);
is('hello, world', $hello, 'db eval');

my $err = $db->eval('function(x) { xreturn "hello, "+x; }', ["world"]);
like($err, qr/(?:compile|execution) failed/, 'js err');

# tie
{
    my $admin = $conn->get_database('admin');
    my %cmd;
    tie( %cmd, 'Tie::IxHash', buildinfo => 1);
    my $result = $admin->run_command(\%cmd);
    is($result->{ok}, 1);
}

END {
    if ($db) {
        $db->drop;
    }
}
