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
use Test::Deep;
use Storable qw( dclone );
use Safe::Isa;

use MongoDB;
use MongoDB::Error;

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

sub clear_events { @events = () }

sub event_cb { push @events, dclone $_[0] }

my $conn = build_client(monitoring_callback => \&event_cb);
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

sub insert_and_check {
    my ($coll, $change_stream, $doc) = @_;
    $coll->insert_one($doc);
    ok(my $change = $change_stream->next, 'got next doc');
    is($change->{'operationType'}, 'insert', 'correct insert op');
    cmp_deeply($change->{'ns'}, {
        'db' => $coll->database->name,
        'coll' => $coll->name,
    });
    ok($change->{'fullDocument'}, 'got full doc');
    return $change;
}

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
            cmp_deeply($change_stream->get_resume_token, $change->{'_id'},
                'track resumeToken');
        }
        is scalar(keys %changed), 10, 'seen all changes';
    };

    subtest 'change streams w/ maxAwaitTimeMS' => sub {
        $coll->drop;
        my $change_stream = $watchable->watch([], { maxAwaitTimeMS => 3000 });
        my $start = time;
        is $change_stream->next, undef, 'next without changes';
        ok(!$change_stream->get_resume_token, 'track resumeToken');
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
        cmp_deeply($change_stream->get_resume_token, $change->{'_id'},
            'track resumeToken');
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
            $change_stream->get_resume_token
        };
        do {
            my $change_stream = $watchable->watch(
                [],
                { resumeAfter => $id },
            );
            cmp_deeply($id, $change_stream->get_resume_token,
                'getResumeToken must return resumeAfter from the initial
                 aggregate if the option was specified.');
            my $change = $change_stream->next;
            ok $change, 'change exists after resume';
            is $change->{fullDocument}{value}, 201,
                'correct change after resume';
            is $change_stream->next, undef, 'no more changes';
        };
    };

    subtest 'change streams w/ startAfter' => sub {
        plan skip_all => 'MongoDB version 4.2 or higher required'
            unless $server_version >= version->parse('v4.1.0'); # 4.2 dev version
        my $id = do {
            my $change_stream = $watchable->watch();
            my $new_name = 'newname' . time();
            $coll->rename($new_name);
            my $change = $change_stream->next;
            ok $change, 'change exists';
            is($change->{'operationType'}, 'rename', 'correct op');
            is($change->{'to'}{'coll'}, $new_name, 'correct new name');
            $change_stream->get_resume_token
        };
        do {
            my $change_stream = $watchable->watch(
                [],
                { startAfter => $id },
            );
            is $change_stream->next, undef, 'no more changes';
        };
    };

    subtest 'change streams w/ CursorNotFound reconnection' => sub {
        $coll->drop;

        my $change_stream = $watchable->watch;

        my $change = insert_and_check($coll, $change_stream, { value => 301 });
        ok $change, 'change received';
        is $change->{fullDocument}{value}, 301, 'correct change';

        $testdb->run_command([
            killCursors => $coll->name,
            cursors => [$change_stream->_result->_cursor_id],
        ]);

        $change = insert_and_check($coll, $change_stream, { value => 302 });
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
        cmp_deeply($change_stream->get_resume_token, $change->{'_id'},
            'track resumeToken');
    };

    subtest 'sessions' => sub {
        skip_unless_sessions();
        clear_events();

        my $session = $conn->start_session;

        my $change_stream = $watchable->watch([], {
            session => $session,
        });

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

    subtest 'PERL-1090' => sub {
        $coll->drop;
        my $change_stream = $watchable->watch([
            { '$project' => { '_id' => 0 } }
        ]);
        is $change_stream->next, undef, 'next without changes';

        $coll->insert_one({});

        my $change = eval { $change_stream->next };
        my $err = $@;
        ok($err, 'ChangeStream must raise an exception');
        ok(
            $err->$_isa('MongoDB::InvalidOperationError')
            || $err->$_isa('MongoDB::DatabaseError'), 'correct exp error');
    };

    subtest 'batchSize is honored' => sub {
        $coll->drop;
        clear_events();

        my $batch_size = { batchSize => 3 };
        my $change_stream = $watchable->watch([], dclone($batch_size));
        is $change_stream->next, undef, 'next without changes';
        ok(!$change_stream->get_resume_token, 'no resume token yet');

        insert_and_check($coll, $change_stream, { '_id' => 1 });
        ok(!$change_stream->next, 'no more changes');

        my $got_event = (
            grep {
                $_->{commandName} eq 'aggregate'
                and $_->{type} eq 'command_started'
            } @events
        )[0];
        cmp_deeply($got_event->{'command'}{'cursor'}, $batch_size);
    };

    subtest 'initial empty batch' => sub {
        $coll->drop;
        my $change_stream = $watchable->watch;
        my $result = $change_stream->_result;
        ok(!$result->has_next, 'The first batch should be empty');
        ok(my $cursor_id = $result->_cursor_id, 'active cursor');

        insert_and_check($coll, $change_stream, {});
        is($cursor_id, $change_stream->_result->_cursor_id,
           'still using same cursor');
    };

    subtest 'postBatchResumeToken' => sub {
        plan skip_all => 'MongoDB version 4.0.7 or higher required'
            unless $server_version >= version->parse('v4.0.7');
        $coll->drop;
        my $id = do {
            my $change_stream = $watchable->watch();
            $coll->insert_one({ value => 200 });
            $coll->insert_one({ value => 201 });
            my $change = $change_stream->next;
            ok $change, 'change exists';
            is $change->{fullDocument}{value}, 200,
                'correct change';
            $change_stream->get_resume_token
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
            my $resume_token = $change_stream->get_resume_token;
            cmp_deeply($resume_token, $change->{'postBatchResumeToken'},
                'track resumeToken');
            is $change_stream->next, undef, 'no more changes';
            cmp_deeply($change_stream->get_resume_token, $resume_token,
                'track resumeToken');
        };
    };

    subtest 'postBatchResumeToken beyond previous batch' => sub {
        plan skip_all => 'MongoDB version 4.0.7 or higher required'
            unless $server_version >= version->parse('v4.0.7');
        $coll->drop;
        my $change_stream = $watchable->watch();
        $coll->insert_one({ value => 200 });
        my $change = $change_stream->next;
        ok $change, 'change exists';
        is $change->{fullDocument}{value}, 200,
            'correct change';
        my $resume_token = $change_stream->get_resume_token;
        ok(!$change_stream->next, 'no more changes');
        # next batch
        $change_stream = $watchable->watch(
            [],
            { resumeAfter => $resume_token },
        );
        $coll->insert_one({ value => 201 });
        $coll->insert_one({ value => 202 });
        cmp_deeply($change_stream->get_resume_token, $resume_token,
            'getResumeToken must return the resume token from
             the previous command response.');
        $change = $change_stream->next;
        isnt($change_stream->get_resume_token->{_data}, $resume_token->{_data});
        isnt($change->{postBatchResumeToken}{_data}, $resume_token->{_data});
    };
}
