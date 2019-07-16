#  Copyright 2017 - present MongoDB, Inc.
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

use MongoDB;
use boolean;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    check_min_server_version
    skip_unless_min_version
/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

# Setup a mapping from option name to '$'-prefixed modifier name
my %modifier_for_option = map { $_ => "\$$_" } qw(
  comment hint max maxScan maxTimeMS min returnKey snapshot
);

# These modifiers have names that differ from the option name
$modifier_for_option{sort}         = '$orderby';
$modifier_for_option{showRecordId} = '$showDiskLoc';

#--------------------------------------------------------------------------#
# Design note: these tests are designed to verify that the various ways
# that legacy query modifiers can be set are faithfully transmitted to
# the server as expected.  For expediency, these tests intercept the command
# rather than try to observe results from the server.
#--------------------------------------------------------------------------#

# Monkey patch MongoDB::Op::_Query to intercept find commands query/command
# constructions and stash them for later analysis.

my @intercept;
{
    no warnings 'redefine';

    my $as_query_document = \&MongoDB::Op::_Query::_as_query_document;
    my $as_command        = \&MongoDB::Op::_Query::_as_command;

    *MongoDB::Op::_Query::_as_query_document = sub {
        push @intercept, scalar $as_query_document->(@_);
        return $intercept[-1];
    };

    *MongoDB::Op::_Query::_as_command = sub {
        push @intercept, scalar $as_command->(@_);
        return $intercept[-1];
    };
}

#--------------------------------------------------------------------------#
# Fixtures
#--------------------------------------------------------------------------#

# How many documents to search for by default
my $num_docs_to_insert = 100;
my $num_docs_to_search = 50;

$coll->delete_many( {} );
$coll->insert_many( [ map { { x => $_ } } 1 .. $num_docs_to_insert ] );

my $index_name = $coll->indexes->create_one( [ x => 1 ] );

#--------------------------------------------------------------------------#
# Test helpers
#--------------------------------------------------------------------------#

sub diag_got_exp {
    my ($g, $e) = @_;
    diag "GOT:\n", explain $g;
    diag "EXP:\n", explain $e;
}

sub option_is {
    my ( $payload, $option_name, $expected ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    # Commands are intecepted as array refs, unlike legacy queries
    $payload = {@$payload} if ref $payload eq 'ARRAY';

    # In a legacy query, options show up as dollar modifiers
    my $key_to_check =
      check_min_server_version($conn, 'v3.2.0') ? $modifier_for_option{$option_name} : $option_name;

    my $got   = $payload->{$key_to_check};
    my $label = "'$key_to_check' correct";

    if ( ref($got) eq 'Tie::IxHash' ) {
        cmp_got_ixhash( $got, $expected, $label );
    }
    else {
        cmp_deeply( $got, $expected, $label ) or diag_got_exp($got, $expected);
    }
}

# If we got a Tie::IxHash object, we need to upgrade the expected value
# similar to how the MongoDB driver does it.
sub cmp_got_ixhash {
    my ( $got, $expected, $label ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;
    $got->[3] = 0; # clear iterator for comparison

    if ( ref($expected) eq 'Tie::IxHash' ) {
        cmp_deeply( $got, $expected, $label ) or diag_got_exp($got, $expected);
    }
    elsif ( ref($expected) eq 'ARRAY' ) {
        my $exp = Tie::IxHash->new(@$expected);
        cmp_deeply( $got, $exp , $label ) or diag_got_exp($got, $exp);
    }
    elsif ( ref($expected) eq 'HASH' ) {
        warn "Comparing multi-key expected hash is unpredictable"
          if keys %$expected > 1;
        my $exp = Tie::IxHash->new(%$expected);
        cmp_deeply( $got, $exp, $label ) or diag_got_exp($got, $exp);
    }
    else {
        die "Don't know how to compare '$got' to '$expected'";
    }
}

#--------------------------------------------------------------------------#
# Tests
#--------------------------------------------------------------------------#

subtest "Given: a query comment" => sub {
    # Intentionally choose a "false-y" comment as a challenge
    my $comment = "0";
    my $comment2 = "1";

    subtest "When: adding a comment via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { comment => $comment } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'comment', $comment );
    };

    subtest "When: adding a comment via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find(
            { x         => { '$gt'      => $num_docs_to_search } },
            { modifiers => { '$comment' => $comment } }
        );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'comment', $comment );
    };

    subtest "When: adding a comment via both options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { comment => $comment, modifiers => { '$comment' => $comment2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'comment', $comment );
    };

};

# Test both string and document hints
my @hints = (
    [ string => $index_name ],
    [ document => [ x => 1 ] ],
);

for my $hint_case ( @hints ) {
    my ($label, $hint) = @$hint_case;
    my $hint2 = "not_really_a_hint";

    subtest "Given: a hint $label" => sub {

        subtest "When: adding a hint via option" => sub {
            @intercept = ();
            my $cursor =
            $coll->find( { x => { '$gt' => $num_docs_to_search } }, { hint => $hint } );
            is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
            option_is( $intercept[-1], 'hint', $hint );
        };

        subtest "When: adding a hint via modifiers" => sub {
            @intercept = ();
            my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
                { modifiers => { '$hint' => $hint } } );
            is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
            option_is( $intercept[-1], 'hint', $hint );
        };

        subtest "When: adding a hint via options and modifiers" => sub {
            @intercept = ();
            my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
                { hint => $hint, modifiers => { '$hint' => $hint2 } } );
            is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
            option_is( $intercept[-1], 'hint', $hint );
        };

        subtest "When: adding a hint via cursor method" => sub {
            @intercept = ();
            my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } } );
            $cursor->hint($hint);
            is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
            option_is( $intercept[-1], 'hint', $hint );
        };
    };
}

subtest "Given: a 'max' value for an index" => sub {
    my $max = { x => $num_docs_to_insert + 1 };
    my $max2 = { x => 0 };

    subtest "When: adding a max via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { max => $max, hint => [x => 1] } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'max', $max );
    };

    subtest "When: adding a max via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { '$max' => $max }, hint => [x => 1] } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'max', $max );
    };

    subtest "When: adding a max via option and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { max => $max, modifiers => { '$max' => $max2 }, hint => [x => 1] } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'max', $max );
    };

};

subtest "Given: a 'maxScan' value for an index" => sub {
    plan skip_all => 'Removed from MongODB 4.1.0+' if $server_version >= v4.1.0;

    my $maxScan = 101;
    my $maxScan2 = 0;

    subtest "When: adding a maxScan via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { maxScan => $maxScan } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxScan', $maxScan );
    };

    subtest "When: adding a maxScan via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { '$maxScan' => $maxScan } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxScan', $maxScan );
    };

    subtest "When: adding a maxScan via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { maxScan => $maxScan, modifiers => { '$maxScan' => $maxScan2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxScan', $maxScan );
    };

};

subtest "Given: a 'maxTimeMS' value for an index" => sub {
    skip_unless_min_version($conn, 'v2.6.0');

    my $maxTimeMS = 1000;
    my $maxTimeMS2 = 2000;

    subtest "When: adding a maxTimeMS via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { maxTimeMS => $maxTimeMS } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxTimeMS', $maxTimeMS );
    };

    subtest "When: adding a maxTimeMS via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { '$maxTimeMS' => $maxTimeMS } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxTimeMS', $maxTimeMS );
    };

    subtest "When: adding a maxTimeMS via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { maxTimeMS => $maxTimeMS, modifiers => { '$maxTimeMS' => $maxTimeMS2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxTimeMS', $maxTimeMS );
    };

    subtest "When: adding a maxTimeMS via cursor method" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } } );
        $cursor->max_time_ms($maxTimeMS);
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'maxTimeMS', $maxTimeMS );
    };
};

subtest "Given: a 'min' value for an index" => sub {
    my $min = { x => -1 };
    my $min2 = { x => 0 };

    subtest "When: adding a min via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { min => $min, hint => [x => 1] } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'min', $min );
    };

    subtest "When: adding a min via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { '$min' => $min }, hint => [x => 1] } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'min', $min );
    };

    subtest "When: adding a min via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { min => $min, modifiers => { '$min' => $min2 }, hint => [x => 1] } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'min', $min );
    };

};

subtest "Given: a 'returnKey' value for an index" => sub {
    my $returnKey = true;
    my $returnKey2 = false;

    subtest "When: adding a returnKey via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { returnKey => $returnKey } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'returnKey', $returnKey );
    };

    subtest "When: adding a returnKey via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { '$returnKey' => $returnKey } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'returnKey', $returnKey );
    };

    subtest "When: adding a returnKey via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { returnKey => $returnKey, modifiers => { '$returnKey' => $returnKey2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'returnKey', $returnKey );
    };
};

subtest "Given: a 'sort' value for an index" => sub {
    my $sort = {  x => -1  };
    my $sort2 = { x => 1 };

    subtest "When: adding a sort via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { sort => $sort } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'sort', $sort );
    };

    subtest "When: adding a sort via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { $modifier_for_option{sort} => $sort } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'sort', $sort );
    };

    subtest "When: adding a sort via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { sort => $sort,  modifiers => { $modifier_for_option{sort} => $sort2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'sort', $sort );
    };

    subtest "When: adding a sort via cursor method" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } } );
        $cursor->sort($sort);
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'sort', $sort );
    };
};

subtest "Given: a 'showRecordId' value for an index" => sub {
    my $showRecordId = true;
    my $showRecordId2 = false;

    subtest "When: adding a showRecordId via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { showRecordId => $showRecordId } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'showRecordId', $showRecordId );
    };

    subtest "When: adding a showRecordId via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { $modifier_for_option{showRecordId} => $showRecordId } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'showRecordId', $showRecordId );
    };

    subtest "When: adding a showRecordId via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { showRecordId => $showRecordId, modifiers => { $modifier_for_option{showRecordId} => $showRecordId2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'showRecordId', $showRecordId );
    };
};

done_testing();
