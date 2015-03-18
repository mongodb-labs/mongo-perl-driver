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

for my $dir ( map { path("t/data/CRUD/$_") } qw/read write/ ) {
    my $iterator = $dir->iterator( { recurse => 1 } );
    while ( my $path = $iterator->() ) {
        next unless -f $path && $path =~ /\.json$/;
        my $plan = eval { decode_json( $path->slurp_utf8 ) };
        if ($@) {
            die "Error decoding $path: $@";
        }

        my $name = $path->relative($dir)->basename(".json");

        subtest $name => sub {
            for my $test ( @{ $plan->{tests} } ) {
                $coll->drop;
                $coll->insert_many( $plan->{data} );
                my $op   = $test->{operation};
                my $meth = $op->{name};
                $meth =~ s{([A-Z])}{_\L$1}g;
                my $test_meth = "test_$meth";
                my $res = main->$test_meth( $test->{description}, $meth, $op->{arguments},
                    $test->{outcome} );
            }
        };
    }
}

#--------------------------------------------------------------------------#
# generic tests
#--------------------------------------------------------------------------#

sub test_read_w_filter {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $filter = delete $args->{filter};
    my $res = $coll->$method( grep { defined } $filter, $args );
    check_read_outcome( $label, $res, $outcome );
}

sub test_write_w_filter {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $filter = delete $args->{filter};
    my $res = $coll->$method( $filter, ( scalar %$args ? $args : () ) );
    if ( $method =~ /^find_one/ ) {
        check_find_one_outcome( $label, $res, $outcome );
    }
    else {
        check_write_outcome( $label, $res, $outcome );
    }
}

sub test_insert {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    $args = delete $args->{document} || delete $args->{documents};
    my $res = $coll->$method($args);
    check_insert_outcome( $label, $res, $outcome );
}

sub test_modify {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $filter = delete $args->{filter};
    my $doc = delete $args->{replacement} || delete $args->{update};
    $args->{returnDocument} = lc( $args->{returnDocument} )
      if exists $args->{returnDocument};
    my $res = $coll->$method( $filter, $doc, ( scalar %$args ? $args : () ) );
    if ( $method =~ /^find_one/ ) {
        check_find_one_outcome( $label, $res, $outcome );
    }
    else {
        check_write_outcome( $label, $res, $outcome );
    }
}

BEGIN {
    *test_find                 = \&test_read_w_filter;
    *test_count                = \&test_read_w_filter;
    *test_delete_many          = \&test_write_w_filter;
    *test_delete_one           = \&test_write_w_filter;
    *test_insert_many          = \&test_insert;
    *test_insert_one           = \&test_insert;
    *test_replace_one          = \&test_modify;
    *test_update_one           = \&test_modify;
    *test_update_many          = \&test_modify;
    *test_find_one_and_delete  = \&test_write_w_filter;
    *test_find_one_and_replace = \&test_modify;
    *test_find_one_and_update  = \&test_modify;
}

#--------------------------------------------------------------------------#
# method-specific tests
#--------------------------------------------------------------------------#

sub test_aggregate {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $pipeline = delete $args->{pipeline};

    # $out not supported until 2.6
    my $is_out = exists $pipeline->[-1]{'$out'};
    return if $is_out && $server_version < v2.6.0;

    # Perl driver returns empty result if $out
    $outcome->{result} = [] if $is_out;

    my $res = $coll->aggregate( grep { defined } $pipeline, $args );
    check_read_outcome( $label, $res, $outcome );
}

sub test_distinct {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $fieldname = delete $args->{fieldName};
    my $filter    = delete $args->{filter};
    my $res       = $coll->distinct( grep { defined } $fieldname, $filter, $args );
    check_read_outcome( $label, $res, $outcome );
}

#--------------------------------------------------------------------------#
# outcome checkers
#--------------------------------------------------------------------------#

sub check_read_outcome {
    my ( $label, $res, $outcome ) = @_;

    if ( ref $outcome->{result} ) {
        my $all = [ $res->all ];
        cmp_deeply( $all, $outcome->{result}, "$label: result documents" )
          or diag explain $all;
    }
    else {
        is( $res, $outcome->{result}, "$label: result scalar" );
    }

    check_collection( $label, $outcome );
}

sub check_write_outcome {
    my ( $label, $res, $outcome ) = @_;

    for my $k ( keys %{ $outcome->{result} } ) {
        ( my $attr = $k ) =~ s{([A-Z])}{_\L$1}g;
        is( $res->$attr, $outcome->{result}{$k}, "$label: $k" );
    }

    check_collection( $label, $outcome );
}

sub check_find_one_outcome {
    my ( $label, $res, $outcome ) = @_;
    cmp_deeply( $res, $outcome->{result}, "$label: result doc" );
    check_collection( $label, $outcome );
}

sub check_insert_outcome {
    my ( $label, $res, $outcome ) = @_;

    if ( exists $outcome->{result}{insertedId} ) {
        return check_write_outcome( $label, $res, $outcome );
    }

    my $ids = [
        map  { $res->inserted_ids->{$_} }
        sort { $a <=> $b } keys %{ $res->inserted_ids }
    ];
    cmp_deeply( $ids, $outcome->{result}{insertedIds}, "$label: result doc" );
    check_collection( $label, $outcome );
}

sub check_collection {
    my ( $label, $outcome ) = @_;

    return unless exists $outcome->{collection};

    my $out_coll =
      exists( $outcome->{collection}{name} )
      ? $testdb->coll( $outcome->{collection}{name} )
      : $coll;

    my $data = [ $out_coll->find( {} )->all ];
    cmp_deeply( $data, $outcome->{collection}{data}, "$label: collection data" )
      or diag explain $data;
}

done_testing;
