#  Copyright 2015 - present MongoDB, Inc.
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

use utf8;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest qw/
    build_client
    get_capped
    get_test_db
    server_version
    server_type
    skip_unless_mongod
/;

skip_unless_mongod();

use MongoDBTest::Callback;

my $cb = MongoDBTest::Callback->new;
my $conn           = build_client(
  monitoring_callback => $cb->callback
);;
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_validating');

my $res;

my $does_validation = $server_version >= v3.1.3;

# only set up validation on servers that support it
sub _drop_coll {
    $coll->drop;
    $testdb->run_command( [ create => $coll->name ] );
    if ($does_validation) {
        $testdb->run_command(
            [ collMod => $coll->name, validator => { x => { '$exists' => 1 } } ] );
    }
    pass("reset collection");
}

subtest "insert_one" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    $cb->clear_events;

    my $not_bypass = exception { $coll->insert_one( {}, { bypassDocumentValidation => 0 } )};

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception { $coll->insert_one( {}, { bypassDocumentValidation => 1 } )};

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest "replace_one" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    my $id = $coll->insert_one( { x => 1 } )->inserted_id;

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->replace_one( { _id => $id }, { y => 1 }, { bypassDocumentValidation => 0 })
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->replace_one( { _id => $id }, { y => 1 }, { bypassDocumentValidation => 1 })
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest "update_one" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    $coll->insert_one( { x => 1 } );

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->update_one(
            { x                        => 1 },
            { '$unset'                 => { x => 1 } },
            { bypassDocumentValidation => 0 }
        )
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->update_one(
            { x                        => 1 },
            { '$unset'                 => { x => 1 } },
            { bypassDocumentValidation => 1 }
        )
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest "update_many" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    $coll->insert_many( [ { x => 1 }, { x => 2 } ] );

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->update_many(
            {},
            { '$unset'                 => { x => 1 } },
            { bypassDocumentValidation => 0 }
          )
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->update_many(
            {},
            { '$unset'                 => { x => 1 } },
            { bypassDocumentValidation => 1 }
          )
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest 'bulk_write (unordered)' => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    my $err;

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->bulk_write(
            [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
            { bypassDocumentValidation => 0, ordered => 0 },
        );
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->bulk_write(
            [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
            { bypassDocumentValidation => 1, ordered => 0 },
        );
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest 'bulk_write (ordered)' => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    my $err;

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->bulk_write(
            [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
            { bypassDocumentValidation => 0, ordered => 1 },
        );
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->bulk_write(
            [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ], ],
            { bypassDocumentValidation => 1, ordered => 1 },
        );
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

# insert_many uses bulk_write internally
subtest "insert_many" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->insert_many( [ {}, {} ], { bypassDocumentValidation => 0 } )
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->insert_many( [ {}, {} ], { bypassDocumentValidation => 1 } )
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest "find_one_and_replace" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    $coll->insert_one( { x => 1 } );

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->find_one_and_replace( { x => 1 }, { y => 1 }, { bypassDocumentValidation => 0 } )
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->find_one_and_replace( { x => 1 }, { y => 1 }, { bypassDocumentValidation => 1 } )
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest "find_one_and_update" => sub {

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    $coll->insert_one( { x => 1 } );

    $cb->clear_events;

    my $not_bypass = exception {
        $coll->find_one_and_update(
            { x                        => 1 },
            { '$unset'                 => { x => 1 } },
            { bypassDocumentValidation => 0 }
          )
    };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception {
        $coll->find_one_and_update(
            { x                        => 1 },
            { '$unset'                 => { x => 1 } },
            { bypassDocumentValidation => 1 }
          )
    };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass , undef, "validation bypassed" );
};

subtest "aggregate with \$out" => sub {
    plan skip_all => "Aggregation with \$out requires MongoDB 2.6+"
        unless $server_version >= v2.6.0;

    plan skip_all => "requires MongoDB 3.2+" unless $does_validation;

    _drop_coll();

    my $source = $testdb->get_collection('test_source');
    $source->insert_many( [ map { { count => $_ } } 1 .. 20 ] );

    $cb->clear_events;

    my $not_bypass = exception {
        $source->aggregate(
            [ { '$match' => { count => { '$gt' => 10 } } }, { '$out' => $coll->name } ],
            { bypassDocumentValidation => 0 } )
        };

    my $err_event = $cb->events->[-2];

    ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation not inserted when false');

    like( $not_bypass, qr/failed validation/, "invalid when false on bypass" );

    $cb->clear_events;

    my $valid_bypass = exception { $source->aggregate(
        [ { '$match' => { count => { '$gt' => 10 } } }, { '$out' => $coll->name } ],
        { bypassDocumentValidation => 1 } ) };

    my $event = $cb->events->[-2];

    ok(exists $event->{ command }->{ 'bypassDocumentValidation' },
        'bypassDocumentValidation inserted when true');

    is( $valid_bypass, undef, "validation bypassed" );

    is( $coll->count_documents({}), 10, "correct doc count in \$out collection" );

    is(
        exception {
            $source->aggregate(
                [ { '$match' => { count => { '$gt' => 10 } } } ],
                { bypassDocumentValidation => 1 } );
        },
        undef,
        "bypassDocumentValidation without \$out",
    );
};

subtest bypass_validation_default => sub {

    my $source = $testdb->get_collection('test_source');

    # For this test we don't use validation on the collection.  We don't
    # care whether it works or not, only that without a bypass option,
    # we don't see a 'bypassDocumentValidation' option sent to the server.
    $coll->drop;

    my %cases = (
        insert_one => sub { $coll->insert_one( {} ) },

        replace_one => sub { $coll->replace_one( { _id => 123 }, { y => 1 } ) },

        update_one => sub { $coll->update_one( { x => 1 }, { '$unset' => { x => 1 } } ) },

        update_many => sub { $coll->update_many( {}, { '$unset' => { x => 1 } } ) },

        bulk_write => sub {
            $coll->bulk_write(
                [ [ insert_one => [ { x => 1 } ] ], [ insert_many => [ {}, { x => 8 } ] ] ] );
        },

        insert_many => sub { $coll->insert_many( [ {}, {} ], ) },

        fine_one_and_replace => sub { $coll->find_one_and_replace( { x => 1 }, { y => 1 } ) },

        find_one_and_update => sub { $coll->find_one_and_update( { x => 1 }, { '$unset' => { x => 1 } } ) },

        aggregate_with_out => sub {
            $source->aggregate(
                [ { '$match' => { count => { '$gt' => 10 } } }, { '$out' => $coll->name } ] );
        },
    );

    for my $k ( sort keys %cases ) {
        $cb->clear_events;

        $cases{$k}->();

        my $err_event = $cb->events->[-2];

        ok( !exists $err_event->{ command }->{ 'bypassDocumentValidation' },
            "$k: bypassDocumentValidation not inserted when default undef");
    }
};

done_testing;
