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
use Test::More;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my @events;
my $conn = build_client(monitoring_callback => sub {
    push @events, shift;
});
my $testdb = get_test_db($conn);
my $coll = $testdb->get_collection('test_collection');

for my $index (0..1000) {
    $coll->insert_one({
        type => 'testval',
        value => $index,
    });
}

my $id;
@events = ();
do {
    my $results = $coll->query({ type => 'testval' });
    ok defined($results->next), 'fetch one document';
    $id = $results->result->_cursor_id;
    ok defined($id), 'cursor id';
    undef $results;
};

my ($event) = grep {
    $_->{commandName} eq 'killCursors'
    &&
    $_->{type} eq 'command_succeeded'
} @events;

ok defined($event), 'successful killcursors event';

if (defined $event and defined $id) {
    is $event->{reply}{ok}, 1,
        'reply ok';
    is_deeply $event->{reply}{cursorsAlive}, [],
        'reply cursors alive';
    is_deeply $event->{reply}{cursorsNotFound}, [],
        'reply cursors not found';
    is_deeply $event->{reply}{cursorsUnknown}, [],
        'reply cursors unknown';
    is_deeply $event->{reply}{cursorsKilled}, [$id],
        'reply cursors killed';
}

done_testing;
