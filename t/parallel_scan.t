#
#  Copyright 2009-2013 MongoDB, Inc.
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
use utf8;
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_version server_type/;

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

# parallel_scan
subtest "parallel scan" => sub {
    plan skip_all => "Parallel scan not supported before MongoDB 2.6"
      unless $server_version >= v2.6.0;
    plan skip_all => "Parallel scan not supported on mongos"
      if $server_type eq 'Mongos';

    my $num_docs = 2000;

    for ( 1 .. $num_docs ) {
        $coll->insert_one( { _id => $_ } );
    }

    my $err_re = qr/must be a positive integer between 1 and 10000/;

    eval { $coll->parallel_scan };
    like( $@, $err_re, "parallel_scan() throws error" );

    for my $i ( 0, -1, 10001 ) {
        eval { $coll->parallel_scan($i) };
        like( $@, $err_re, "parallel_scan($i) throws error" );
    }

    my $max     = 3;
    my @cursors = $coll->parallel_scan($max);
    ok( scalar @cursors <= $max, "parallel_scan($max) returned <= $max cursors" );

    for my $method (qw/reset count explain/) {
        eval { $cursors[0]->$method };
        like(
            $@,
            qr/Can't locate object method/,
            "$method on parallel scan cursor throws error"
        );
    }

    _check_parallel_results( $num_docs, @cursors );

    # read preference
    subtest "replica set" => sub {
        plan skip_all => 'needs a replicaset'
          unless $server_type eq 'RSPrimary';

        my $conn2 = MongoDBTest::build_client(
            read_preference => 'secondaryPreferred'
        );

        my @cursors = $coll->parallel_scan($max);
        _check_parallel_results( $num_docs, @cursors );
    };

    # empty collection
    subtest "empty collection" => sub {
        $coll->delete_many({});
        my @cursors = $coll->parallel_scan($max);
        _check_parallel_results( 0, @cursors );
      }

};

sub _check_parallel_results {
    my ( $num_docs, @cursors ) = @_;
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my %seen;
    my $count = 0;
    for my $i ( 0 .. $#cursors ) {
        my @chunk = $cursors[$i]->all;
        if ($num_docs) {
            ok( @chunk > 0, "cursor $i had some results" );
        }
        else {
            is( scalar @chunk, 0, "cursor $i had no results" );
        }
        $seen{$_}++ for map { $_->{_id} } @chunk;
        $count += @chunk;
    }
    is( $count, $num_docs, "cursors returned right number of docs" );
    is_deeply(
        [ sort { $a <=> $b } keys %seen ],
        [ 1 .. $num_docs ],
        "cursors returned all results"
    );

}

done_testing;
