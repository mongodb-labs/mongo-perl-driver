#
#  Copyright 2015 MongoDB, Inc.
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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type get_capped/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

plan skip_all => "Requires MongoDB 3.6"
    if $server_version < v3.6.0;

# Last in First out
subtest 'LIFO Pool' => sub {
    my $session_a = $conn->start_session;
    my $session_b = $conn->start_session;

    # cache the ID's
    my $id_a = $session_a->session_id;
    my $id_b = $session_b->session_id;
    ok defined $id_a->{id}, 'Session A ID defined';
    ok defined $id_b->{id}, 'Session B ID defined';

    $session_a->end_session;
    $session_b->end_session;

    # Internal only check, not part of spec
    is $session_a->server_session, undef, 'Session A has been returned';
    is $session_b->server_session, undef, 'Session B has been returned';

    my $session_c = $conn->start_session;
    ok defined $session_c->session_id->{id}, 'Session C ID defined';
    is $session_c->session_id->{id}, $id_b->{id}, 'Session C same ID as Session B';

    my $session_d = $conn->start_session;
    ok defined $session_d->session_id->{id}, 'Session D ID defined';
    is $session_d->session_id->{id}, $id_a->{id}, 'Session D same ID as Session A';
};

subtest 'clusterTime in commands' => sub {
    # Need a new client with high heartbeatFrequencyMS
    my $local_client = build_client(
        # You want big number? we give you big number
        heartbeat_frequency_ms => 9_000_000_000,
    );

    use Test::Role::CommandDebug;

    Role::Tiny->apply_roles_to_package(
        'MongoDB::Op::_Command', 'Test::Role::CommandDebug',
    );

    use Devel::Dwarn;

    #Dwarn $local_client;

    subtest 'ping' => sub {
        my $ping_result = $local_client->send_admin_command(Tie::IxHash->new('ping' => 1));

        my $command = shift @Test::Role::CommandDebug::COMMAND_QUEUE;

        ok $command->query->EXISTS('ping'), 'ping in sent command';

        # TODO check maxWireVersion
        ok $command->query->EXISTS('$clusterTime'), 'clusterTime in sent command';
        Dwarn $ping_result;

        my $ping_result2 = $local_client->send_admin_command(Tie::IxHash->new('ping' => 1));
        Dwarn $ping_result2;
    };

};

done_testing;
