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
use Test::Deep qw/!blessed/;

use utf8;
use Tie::IxHash;
use Encode qw(encode decode);
use MongoDB::Error;

use MongoDB;
use BSON::Types ':all';

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type/;

$ENV{PERL_MONGO_NO_DEP_WARNINGS} = 1;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type = server_type($conn);
my $coll = $testdb->get_collection('test_collection');

my $supports_collation = $server_version >= 3.3.9;
my $case_insensitive_collation = { locale => "en_US", strength => 2 };

subtest "count" => sub {
    $coll->drop;
    $coll->insert_one( { i => 1 } );
    $coll->insert_one( { i => 2 } );
    is ($coll->count(), 2, 'count = 2');
    is ($coll->count({i => 2}), 1, 'count(filter) = 1');

    # missing collection
    my $coll2 = $testdb->coll("aadfkasfa");
    my $count;
    is(
        exception { $count = $coll2->count({}) },
        undef,
        "count on missing collection lives"
    );
    is( $count, 0, "count is correct" );
};

subtest "count w/ hint" => sub {

    $coll->drop;
    $coll->insert_one( { i => 1 } );
    $coll->insert_one( { i => 2 } );
    is ($coll->count(), 2, 'count = 2');

    $coll->indexes->create_one( { i => 1 } );

    is( $coll->count( { i => 1 }, { hint => '_id_' } ), 1, 'count w/ hint & spec');
    is( $coll->count( {}, { hint => '_id_' } ), 2, 'count w/ hint');

    my $current_version = version->parse($server_version);
    my $version_2_6 = version->parse('v2.6');

    if ( $current_version > $version_2_6 ) {

        eval { $coll->count( { i => 1 } , { hint => 'BAD HINT' } ) };
        like($@, qr/failed|bad hint|hint provided does not correspond/, 'check bad hint error');

    } else {

        is( $coll->count( { i => 1 } , { hint => 'BAD HINT' } ), 1, 'bad hint and spec');
    }

    $coll->indexes->create_one( { x => 1 }, { sparse => 1 } );

    if ($current_version > $version_2_6 ) {

        is( $coll->count( {  i => 1 } , { hint => 'x_1' } ), 0, 'spec & hint on empty sparse index');

    } else {

        is( $coll->count( {  i => 1 } , { hint => 'x_1' } ), 1, 'spec & hint on empty sparse index');
    }

    # XXX Failing on nightly master -- xdg, 2016-02-11
    TODO: {
        local $TODO = "Failing nightly master";
        is( $coll->count( {}, { hint => 'x_1' } ), 2, 'hint on empty sparse index');
    }
};

subtest "count w/ collation" => sub {
    $coll->drop;
    $coll->insert_one( { x => "foo" } );

    if ($supports_collation) {
        is( $coll->count( { x => "FOO" }, { collation => $case_insensitive_collation } ),
            1, 'count w/ collation' );
    }
    else {
        like(
            exception {
                $coll->count( { x => "FOO" }, { collation => $case_insensitive_collation } );
            },
            qr/MongoDB host '.*:\d+' doesn't support collation/,
            "count w/ collation returns error if unsupported"
        );
    }
};

done_testing;
