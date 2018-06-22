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

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    skip_unless_sessions
    uuid_to_string
    build_client
    get_test_db
    server_version
    server_type
/;

skip_unless_mongod();

my @events;

my $conn = build_client(monitoring_callback => sub {
    push @events, shift;
});

my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);
my $coll = $testdb->get_collection('test_collection');

plan skip_all => 'MongoDB replica set required'
    unless $server_type eq 'RSPrimary';

subtest 'client' => sub {
    plan skip_all => 'MongoDB version 4.0 or higher required'
        unless $server_version >= version->parse('v4.0.0');
    run_tests_for($conn);
};

subtest 'database' => sub {
    plan skip_all => 'MongoDB version 4.0 or higher required'
        unless $server_version >= version->parse('v4.0.0');
    run_tests_for($testdb);
};

subtest 'collection' => sub {
    plan skip_all => 'MongoDB version 3.6 or higher required'
        unless $server_version >= version->parse('v3.6.0');
    run_tests_for($coll);
};

done_testing;

sub run_tests_for {
    my ($watchable) = @_;

    subtest 'basic' => sub {
        $coll->drop;
        $coll->insert_one({ value => 1 });
        $coll->insert_one({ value => 2 });

        my $change_stream = $watchable->watch();
        is $change_stream->next, undef, 'next without changes';

        for my $index (1..10) {
            $coll->insert_one({ value => 100 + $index });
        }
        my %changed;
        while (my $change = $change_stream->next) {
            is $changed{ $change->{fullDocument}{value} }++, 0,
                'first seen '.$change->{fullDocument}{value};
        }
        is scalar(keys %changed), 10, 'seen all changes';
    };

    subtest 'change streams w/ maxAwaitTimeMS' => sub {
        $coll->drop;
        my $change_stream = $watchable->watch([], { maxAwaitTimeMS => 3000 });
        my $start = time;
        is $change_stream->next, undef, 'next without changes';
        my $elapsed = time - $start;
        my $min_elapsed = 2;
        ok $elapsed > $min_elapsed, "waited for at least $min_elapsed secs";
    };

    subtest 'change streams w/ fullDocument' => sub {
        $coll->drop;
        $coll->insert_one({ value => 1 });
        my $change_stream = $watchable->watch(
            [],
            { fullDocument => 'updateLookup' },
        );
        $coll->update_one(
            { value => 1 },
            { '$set' => { updated => 3 }},
        );
        my $change = $change_stream->next;
        is $change->{operationType}, 'update', 'change is an update';
        ok exists($change->{fullDocument}), 'delta contains full document';
    };

    subtest 'change streams w/ resumeAfter' => sub {
        $coll->drop;
        my $id = do {
            my $change_stream = $watchable->watch();
            $coll->insert_one({ value => 200 });
            $coll->insert_one({ value => 201 });
            my $change = $change_stream->next;
            ok $change, 'change exists';
            is $change->{fullDocument}{value}, 200,
                'correct change';
            $change->{_id}
        };
        do {
            my $change_stream = $watchable->watch(
                [],
                { resumeAfter => $id },
            );
            my $change = $change_stream->next;
            ok $change, 'change exists after resume';
            is $change->{fullDocument}{value}, 201,
                'correct change after resume';
            is $change_stream->next, undef, 'no more changes';
        };
    };

    subtest 'change streams w/ CursorNotFound reconnection' => sub {
        $coll->drop;

        my $change_stream = $watchable->watch;
        $coll->insert_one({ value => 301 });
        my $change = $change_stream->next;
        ok $change, 'change received';
        is $change->{fullDocument}{value}, 301, 'correct change';

        $testdb->run_command([
            killCursors => $coll->name,
            cursors => [$change_stream->_result->_cursor_id],
        ]);

        $coll->insert_one({ value => 302 });
        $change = $change_stream->next;
        ok $change, 'change received after reconnect';
        is $change->{fullDocument}{value}, 302, 'correct change';
    };

    subtest 'startAtOperationTime' => sub {
        plan skip_all => 'MongoDB version 4.0 or higher required'
            unless $server_version >= version->parse('v4.0.0');

        $coll->drop;

        my $change_stream = $watchable->watch([], {
            startAtOperationTime => scalar(time + 3),
        });
        $coll->insert_one({ value => 401 });
        sleep 4;
        $coll->insert_one({ value => 402 });

        my $change = $change_stream->next;
        ok $change, 'change received';
        is $change->{fullDocument}{value}, 402, 'correct change';

        ok !defined($change_stream->next), 'no more changes';
    };

    subtest 'sessions' => sub {
        skip_unless_sessions();
        @events = ();

        my $session = $conn->start_session;

        my $change_stream = $watchable->watch([], {
            session => $session,
        });
        $change_stream->next;

        my ($event) = grep {
            $_->{commandName} eq 'aggregate' and
            $_->{type} eq 'command_started'
        } @events;

        ok(defined($event), 'found event')
            or return;

        my $lsid = uuid_to_string(
            $session->_server_session->session_id->{id}->data,
        );

        my $command = $event->{command};
        my $command_sid = uuid_to_string($command->{lsid}{id}->data);

        is $command_sid, $lsid, 'command has correct session id';
    };
}
