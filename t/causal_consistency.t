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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
    skip_unless_mongod
    skip_unless_sessions
/;

skip_unless_mongod();
skip_unless_sessions();

my @events;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }

my $conn           = build_client(
  monitoring_callback => \&event_cb,
);
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

# spec test 1
subtest 'session operation_time undef on init' => sub {
    my $session = $conn->start_session;
    is $session->operation_time, undef, 'is undef';
};

clear_events();

# spec test 2
subtest 'first read' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    my $response = $coll->find_one({ _id => 1 }, {}, { session => $session });

    my $event = $events[-2];

    ok ! exists $event->{ command }->{ 'afterClusterTime' }, 'afterClusterTime not sent on first read';
};

clear_events();

# spec test 3
subtest 'update operation_time' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    is $session->operation_time, undef, 'Empty operation time';

    my $response = $coll->insert_one({ _id => 1 }, { session => $session });

    my $event = $events[-1];

    # for some reason 'is' wont do the comparison correctly...
    ok $session->operation_time == $event->{reply}->{operationTime}, 'response has operation time and is updated in session';

    $session->end_session;

    $session = $conn->start_session({ causalConsistency => 1 });

    is $session->operation_time, undef, 'Empty operation time';

    # Try inserting the same thing again (_id must be unique in a collection afaik)
    my $err = exception { $coll->insert_one({ _id => 1 }, { session => $session }) };

    isa_ok( $err, 'MongoDB::DatabaseError', "duplicate insert error" );

    my $err_event = $events[-1];

    ok $session->operation_time == $err_event->{reply}->{operationTime}, 'response has operation time and is updated in session';
};

clear_events();

# spec test 4
subtest 'find_one then read includes operationtime' => sub {
    # run for all read ops:
    # * find
    # * find_one
    # * find_id
    # * aggregate
    # * count
    # * distinct

    my $tests = {
        find            => [ {_id => 1 } ],
        find_one        => [ { _id => 1 }, {} ],
        find_id         => [ 1, {} ],
        aggregate       => [ [ { '$match' => { count => { '$gt' => 0 } } } ] ],
        count_documents => [ { _id => 1 } ],
        distinct        => [ "id_", { _id => 1 } ],
    };

    for my $key ( qw/
      find
      find_one
      find_id
      aggregate
      count_documents
      distinct / ) {
        clear_events();
        subtest $key => sub {
            my $session = find_one_and_get_session();
            my $op_time = $session->operation_time;

            my $ret = $coll->$key(@{ $tests->{$key} }, { session => $session });
            if ( $key eq 'find' ) { $ret->result }

            my $event = $events[-2];

            is $op_time, $event->{command}->{'readConcern'}->{afterClusterTime}, 'has correct afterClusterTime';
        };
    }
};

sub find_one_and_get_session {
    my $session = $conn->start_session({ causalConsistency => 1 });

    my $find = $coll->find_one({ _id => 1 }, {}, { session => $session });

    return $session;
}

clear_events();

# spec test 5
subtest 'write then find_one includes operationTime' => sub {
    # repeat for all write ops:
    # * insert_one
    # * insert_many
    # * delete_one
    # * delete_many
    # * replace_one
    # * update_one
    # * update_many
    # * find_one_and_delete
    # * find_one_and_replace
    # * find_one_and_update
    # * ordered_bulk
    # * unordered_bulk

    # Undef exceptions are only due to not knowing how to cause one
    my $tests = {
        insert_one => {
            success => [ { _id => 100 } ],
            exception => [ { _id => 100 } ],
        },
        insert_many => {
            success => [ [ map { { _id => $_ } } 101..111 ] ],
            exception => [ [ map { { _id => $_ } } 101..111 ] ],
        },
        delete_one => {
            success => [ { _id => 100 } ],
            exception => undef,
        },
        delete_many => {
            success => [ { _id => { '$in' => [101,102] } } ],
            exception => undef,
        },
        replace_one => {
            success => [ { _id => 103 }, { _id => 103, foo => 'qux' } ],
            exception => undef,
        },
        update_one => {
            success => [ { _id => 104 }, { '$set' => { bar => 'baz' } } ],
            exception => undef,
        },
        update_many => {
            success => [ { _id => { '$in' => [105,106] } }, { '$set' => { bar => 'baz' } } ],
            exception => undef,
        },
        find_one_and_delete => {
            success => [ { _id => 107 } ],
            exception => undef,
        },
        find_one_and_replace => {
            success => [ { _id => 108 }, { _id => 108, bar => 'baz' } ],
            exception => undef,
        },
        find_one_and_update => {
            success => [ { _id => 109 }, { '$set' => { foo => 'qux' } } ],
            exception => undef,
        },
    };

    # Order of these actually matters - the insert_one and insert_many must go first
    for my $key ( qw/
      insert_one
      insert_many
      delete_one
      delete_many
      replace_one
      update_one
      update_many
      find_one_and_delete
      find_one_and_replace
      find_one_and_update / ) {
        clear_events();
        subtest $key => sub {
            my $session = $conn->start_session({ causalConsistency => 1 });

            $coll->$key( @{ $tests->{ $key }->{ success } }, { session => $session });

            find_one_and_assert( $session );

            return unless defined $tests->{ $key }->{ exception };

            $session->end_session;

            $session = $conn->start_session({ causalConsistency => 1 });

            my $err = exception {
                $coll->$key( @{ $tests->{ $key }->{ exception } }, { session => $session })
            };

            isa_ok( $err, 'MongoDB::DatabaseError' );

            find_one_and_assert( $session );
        };
    }

    subtest 'ordered_bulk' => sub {
        my $session = $conn->start_session({ causalConsistency => 1 });

        my $bulk = $coll->ordered_bulk;
        $bulk->insert_one({ _id => 120 });
        $bulk->insert_one({ _id => 121 });
        $bulk->execute(undef, { session => $session });

        find_one_and_assert( $session );

        $session->end_session;

        $session = $conn->start_session({ causalConsistency => 1 });

        my $err = exception {
            my $bulk2 = $coll->ordered_bulk;
            $bulk2->insert_one({ _id => 120 });
            $bulk2->insert_one({ _id => 121 });
            $bulk2->execute(undef, { session => $session });
        };
        isa_ok( $err, 'MongoDB::DatabaseError' );

        find_one_and_assert( $session );
    };

    subtest 'unordered_bulk' => sub {
        my $session = $conn->start_session({ causalConsistency => 1 });

        my $bulk = $coll->unordered_bulk;
        $bulk->insert_one({ _id => 123 });
        $bulk->insert_one({ _id => 124 });
        $bulk->execute(undef, { session => $session });

        find_one_and_assert( $session );

        $session->end_session;

        $session = $conn->start_session({ causalConsistency => 1 });

        my $err = exception {
            my $bulk2 = $coll->unordered_bulk;
            $bulk2->insert_one({ _id => 123 });
            $bulk2->insert_one({ _id => 124 });
            $bulk2->execute(undef, { session => $session });
        };
        isa_ok( $err, 'MongoDB::DatabaseError' );

        find_one_and_assert( $session );
    };
};

sub find_one_and_assert {
    my $session = shift;
    my $op_time = $session->operation_time;

    ok defined $op_time, 'got operationTime in session';

    $coll->find_one({ _id => 1 }, {}, { session => $session });

    my $event = $events[-2];

    is $op_time, $event->{command}->{'readConcern'}->{afterClusterTime}, 'has correct afterClusterTime';
}

clear_events();

# spec test 6
subtest 'turn off causalConsistency' => sub {
    my $session = $conn->start_session({ causalConsistency => 0 });

    $coll->find_one({ _id => 1 }, {}, { session => $session });

    $coll->find_one({ _id => 1 }, {}, { session => $session });

    my $event = $events[-2];

    ok ! exists $event->{command}->{'readConcern'}, 'no readconcern, so no afterClusterTime';
};

clear_events();

# spec test 8
subtest 'using default readConcern' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    # collection uses server ReadConcern by default
    $coll->find_one({ _id => 1 }, {}, { session => $session });

    my $op_time = $session->operation_time;

    $coll->find_one({ _id => 1 }, {}, { session => $session });

    my $event = $events[-2];

    ok ! defined $event->{command}->{'readConcern'}->{level}, 'no read concern level with default value';
};

clear_events();

# spec test 9
subtest 'using custom readConcern' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    my $custom_coll = get_unique_collection( $testdb, 'custom_readconcern', { read_concern => { level => 'local' } } );
    # collection uses server ReadConcern by default
    $custom_coll->find_one({ _id => 1 }, {}, { session => $session });

    my $op_time = $session->operation_time;

    $custom_coll->find_one({ _id => 1 }, {}, { session => $session });

    my $event = $events[-2];

    my $read_concern = $event->{command}->{'readConcern'};

    is $read_concern->{level}, 'local', 'read concern level with custom value';
    is $read_concern->{afterClusterTime}, $op_time, 'read concern afterClusterTime present';
};

#spec test 10
subtest 'unacknowledged writes' => sub {
    my $session = $conn->start_session({ causalConsistency => 1 });

    my $custom_coll = get_unique_collection( $testdb, 'unac_write', { write_concern => { w => 0 } } );

    $custom_coll->update_one({ _id => 1 }, { '$set' => { 'manamana' => 'doo dooo doo doodoo' } }, { session => $session });

    ok ! defined $session->operation_time, 'no operation time set from unac write';
};

clear_testdbs;

done_testing;
