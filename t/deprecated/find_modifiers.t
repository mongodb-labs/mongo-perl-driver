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
use MongoDBTest
  qw/skip_unless_mongod build_client get_test_db server_version server_type/;

$ENV{PERL_MONGO_NO_DEP_WARNINGS} = 1;

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

sub option_is {
    my ( $payload, $option_name, $expected ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    # Commands are intecepted as array refs, unlike legacy queries
    $payload = {@$payload} if ref $payload eq 'ARRAY';

    # In a legacy query, options show up as dollar modifiers
    my $key_to_check =
      $server_version < v3.2.0 ? $modifier_for_option{$option_name} : $option_name;

    my $got   = $payload->{$key_to_check};
    my $label = "'$key_to_check' correct";

    if ( ref($got) eq 'Tie::IxHash' ) {
        cmp_got_ixhash( $got, $expected, $label );
    }
    else {
        cmp_deeply( $got, $expected, $label );
    }
}

# If we got a Tie::IxHash object, we need to upgrade the expected value
# similar to how the MongoDB driver does it.
sub cmp_got_ixhash {
    my ( $got, $expected, $label ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    if ( ref($expected) eq 'Tie::IxHash' ) {
        cmp_deeply( $got, $expected, $label );
    }
    elsif ( ref($expected) eq 'ARRAY' ) {
        cmp_deeply( $got, Tie::IxHash->new(@$expected), $label );
    }
    elsif ( ref($expected) eq 'HASH' ) {
        warn "Comparing multi-key expected hash is unpredictable"
          if keys %$expected > 1;
        cmp_deeply( $got, Tie::IxHash->new(%$expected), $label );
    }
    else {
        die "Don't know how to compare '$got' to '$expected'";
    }
}

#--------------------------------------------------------------------------#
# Tests
#--------------------------------------------------------------------------#

subtest "Given: a 'snapshot' value for an index" => sub {
    plan skip_all => "Snapshot removed in 3.7+"
        unless $server_version < v3.7.0;

    my $snapshot = true;
    my $snapshot2 = false;

    subtest "When: adding a snapshot via option" => sub {
        @intercept = ();
        my $cursor =
          $coll->find( { x => { '$gt' => $num_docs_to_search } }, { snapshot => $snapshot } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'snapshot', $snapshot );
    };

    subtest "When: adding a snapshot via modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { modifiers => { '$snapshot' => $snapshot } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'snapshot', $snapshot );
    };

    subtest "When: adding a snapshot via options and modifiers" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } },
            { snapshot => $snapshot, modifiers => { '$snapshot' => $snapshot2 } } );
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'snapshot', $snapshot );
    };

    subtest "When: adding a snapshot via cursor method" => sub {
        @intercept = ();
        my $cursor = $coll->find( { x => { '$gt' => $num_docs_to_search } } );
        $cursor->snapshot($snapshot);
        is( scalar $cursor->all(), $num_docs_to_search, "Number of documents correct" );
        option_is( $intercept[-1], 'snapshot', $snapshot );
    };
};

done_testing();
