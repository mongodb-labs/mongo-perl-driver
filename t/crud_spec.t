#
#  Copyright 2015 MongoDB, Inc.
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
use Test::More 0.96;
use JSON::MaybeXS;
use Test::Deep;
use Path::Tiny;
use Try::Tiny;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_version server_type get_capped/;

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

subtest "read tests" => sub {
    my $iterator = path('t/data/CRUD/read')->iterator( { recurse => 1 } );

    while ( my $path = $iterator->() ) {
        next unless -f $path && $path =~ /\.json$/;
        my $plan = eval { decode_json( $path->slurp_utf8 ) };
        if ($@) {
            die "Error decoding $path: $@";
        }

        my $name = $path->relative('t/data/CRUD/read')->basename(".json");

        subtest $name => sub {
            $coll->drop;
            $coll->insert_many( $plan->{data} );

            for my $test ( @{ $plan->{tests} } ) {
                subtest $test->{description} => sub {
                    my $op   = $test->{operation};
                    my $meth = "test_$op->{name}";
                    my $res  = main->$meth( $op->{arguments}, $test->{outcome} );
                };
            }
        };
    }
};

sub test_aggregate {
    my ( $class, $args, $outcome ) = @_;
    my $pipeline = delete $args->{pipeline};

    # $out not supported until 2.6
    my $is_out = exists $pipeline->[-1]{'$out'};
    return if $is_out && $server_version < v2.6.0;

    # Perl driver returns empty result if $out
    $outcome->{result} = [] if $is_out;

    my $res = $coll->aggregate( grep { defined } $pipeline, $args );
    check_outcome( $res, $outcome );
}

sub test_count {
    my ( $class, $args, $outcome ) = @_;
    my $filter = delete $args->{filter};
    my $res = $coll->count( grep { defined } $filter, $args );
    check_outcome( $res, $outcome );
}

sub test_distinct {
    my ( $class, $args, $outcome ) = @_;
    my $fieldname = delete $args->{fieldName};
    my $filter    = delete $args->{filter};
    my $res       = $coll->distinct( grep { defined } $fieldname, $filter, $args );
    check_outcome( $res, $outcome );
}

sub test_find {
    my ( $class, $args, $outcome ) = @_;
    my $filter = delete $args->{filter};
    my $res = $coll->find( grep { defined } $filter, $args );
    check_outcome( $res, $outcome );
}

sub check_outcome {
    my ( $res, $outcome ) = @_;

    if ( ref $outcome->{result} ) {
        my $all = [ $res->all ];
        cmp_deeply( $all, $outcome->{result}, "result documents" )
          or diag explain $all;
    }
    else {
        is( $res, $outcome->{result}, "result scalar" );
    }

    return unless exists $outcome->{collection};

    my $out_coll =
      exists( $outcome->{collection}{name} )
      ? $testdb->coll( $outcome->{collection}{name} )
      : $coll;

    my $data = [ $out_coll->find( {} )->all ];
    cmp_deeply( $data, $outcome->{collection}{data}, "collection data" )
      or diag explain $data;
}

done_testing;
