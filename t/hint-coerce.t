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

use MongoDB;
use MongoDB::Error;
use BSON::Types ':all';

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type get_capped/;

skip_unless_mongod();

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

plan skip_all => "hints unsupported on MongoDB $server_version"
    unless $server_version >= v3.6.0;

$coll->insert_many( [
  { _id => 1, category => "cake", type => "chocolate",    qty => 10 },
  { _id => 2, category => "cake", type => "ice cream",    qty => 25 },
  { _id => 3, category => "pie",  type => "boston cream", qty => 20 },
  { _id => 4, category => "pie",  type => "blueberry",    qty => 15 },
] );

$coll->indexes->create_one( [ qty => 1, type => 1 ] );
my $index_name = $coll->indexes->create_one( [qty => 1, category => 1 ] );

subtest "no hint" => sub {
  test_hints();
};

subtest "hint string" => sub {
  test_hints( $index_name );
};

subtest "hint array" => sub {
  test_hints( [ qty => 1, category => 1 ] );
};

subtest "hint IxHash" => sub {
  test_hints( Tie::IxHash->new( qty => 1, category => 1 ) );
};

subtest "hint BSON::Doc" => sub {
  test_hints( bson_doc( qty => 1, category => 1 ) );
};

sub test_hints {
  my $hint = shift;
  test_aggregate( $hint );
  test_count_documents( $hint );
  test_find( $hint );
  test_cursor( $hint );
}

sub test_aggregate {
  my $hint = shift;

  subtest 'aggregate' => sub {
    my $cursor = $coll->aggregate(
        [
            { '$sort' => { qty => 1 } },
            { '$match' => { category => 'cake', qty => 10 } },
            { '$sort' => { type => -1 } } ],
        { ( defined $hint ? ( hint => $hint ) : () ), explain => 1 }
    );

    my $result = $cursor->next;

    is( ref( $result ), 'HASH', "aggregate with explain returns a hashref" );

    if ( defined $hint ) {
      ok(
          scalar( @{ $result->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) == 0,
          "aggregate with hint had no rejectedPlans",
      );
    } else {
      ok(
          scalar( @{ $result->{stages}->[0]->{'$cursor'}->{queryPlanner}->{rejectedPlans} } ) > 0,
          "aggregate with no hint had rejectedPlans",
      );
    }
  };
}

sub test_count_documents {
  my $hint = shift;

  subtest 'count_document' => sub {
    is(
      $coll->count_documents(
        { category => 'cake', qty => { '$gt' => 0 } },
        { ( defined $hint ? ( hint => $hint ) : () ) } ),
      2,
      'count w/ spec' );
    is(
      $coll->count_documents(
        {},
        { ( defined $hint ? ( hint => $hint ) : () ) } ),
      4,
      'count' );
  };
}

sub test_find {
  my $hint = shift;

  subtest 'find' => sub {
    # XXX cant use explain here to check that its actually using the hint
    my $cursor = $coll->find(
      { category => 'cake', qty => { '$gt' => 15 } },
      { ( defined $hint ? ( hint => $hint ) : () ) }
    );

    my @res = $cursor->all;

    cmp_deeply \@res,
      [
        {
          _id => 2,
          category => "cake",
          qty => 25,
          type => "ice cream",
        },
      ],
      'Got correct result';
  }
}

sub test_cursor {
  my $hint = shift;

  subtest 'cursor' => sub {
    # Actually the same as find, just setting the hint after the fact
    my $cursor = $coll->find(
      { category => 'cake', qty => { '$gt' => 15 } }
    );

    $cursor->hint( $hint ) if defined $hint;

    my @res = $cursor->all;

    cmp_deeply \@res,
      [
        {
          _id => 2,
          category => "cake",
          qty => 25,
          type => "ice cream",
        },
      ],
      'Got correct result';
  }
}

done_testing;
