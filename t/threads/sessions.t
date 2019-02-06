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
use Config;
use if $Config{usethreads}, 'threads';
use Test::More;

BEGIN { plan skip_all => 'requires threads' unless $Config{usethreads} }

BEGIN { plan skip_all => 'threads not supported before Perl 5.8.5' unless $] ge "5.008005" }

BEGIN { plan skip_all => 'threads tests flaky on older Windows Perls' if $^O eq "MSWin32" && $] lt "5.020000" }

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod skip_unless_sessions build_client get_test_db/;

skip_unless_mongod();
skip_unless_sessions();

my $client = build_client();
my $testdb = get_test_db($client);

# Session test #11: test that pool can be cleared

subtest "clear the session pool" => sub {
    my $session = $client->start_session;
    my $lsid    = $session->session_id->{id};
    $session->end_session;

    threads->create(
        sub {
            my $session = shift;
            $client->reconnect;
            my $session2 = $client->start_session;
            isnt( $session2->session_id->{id}, $lsid, "child got new session id" );
        },
        $session
    )->join;

    my $session2 = $client->start_session;
    is( $session2->session_id->{id}, $lsid, "parent cached session id" );
};

# Session test #12: test that pool won't accept old sessions after reset.
# This is just like #11 except the initial session is ended in both the
# parent *and* the child thread.

subtest "pool has epochs" => sub {
    my $session = $client->start_session;
    my $lsid    = $session->session_id->{id};

    threads->create(
        sub {
            my $session = shift;
            $client->reconnect;
            $session->end_session;
            my $session2 = $client->start_session;
            isnt( $session2->session_id->{id}, $lsid, "child got new session id" );
        },
        $session
    )->join;

    $session->end_session;
    my $session2 = $client->start_session;
    is( $session2->session_id->{id}, $lsid, "parent cached session id" );
};

done_testing();
