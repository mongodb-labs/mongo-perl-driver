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
use JSON::MaybeXS;
use Test::Deep;
use Path::Tiny;
use version;

use MongoDB;
use boolean;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    get_features
    check_min_server_version
    skip_unless_min_version
/;

skip_unless_mongod();

plan skip_all => "Not testing with BSON wrappers"
  if $ENV{PERL_MONGO_TEST_CODEC_WRAPPED};

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_type    = server_type($conn);
my $features       = get_features($conn);
my $coll           = $testdb->get_collection('test_collection');


for my $dir ( map { path("t/data/CRUD/v2/$_") } qw/read write/ ) {
    my $iterator = $dir->iterator( { recurse => 1 } );
    while ( my $path = $iterator->() ) {
        next unless -f $path && $path =~ /\.json$/;
        my $plan = eval { decode_json( $path->slurp_utf8 ) };
        if ($@) {
            die "Error decoding $path: $@";
        }

        my $name = $path->relative($dir)->basename(".json");

        subtest $name => sub {
            if ( $name =~ 'arrayFilter' && ! $features->supports_arrayFilters ) {
                plan skip_all => "arrayFilters not supported on this mongod";
            }
            if ( exists $plan->{minServerVersion} ) {
                my $min_version = $plan->{minServerVersion};
                skip_unless_min_version( $conn, $min_version );
            }
            for my $test ( @{ $plan->{tests} } ) {
                $coll->drop;
                $coll->insert_many( $plan->{data} );
                foreach my $op ( @{ $test->{'operations'} || [$test->{'operation'}] } ) {
                    my $meth   = $op->{name};
                    my $object = $op->{'object'} || 'collection';
                    local $ENV{PERL_MONGO_NO_DEP_WARNINGS} = 1 if $meth eq 'count';
                    $meth =~ s{([A-Z])}{_\L$1}g;
                    my $test_meth = "test_${meth}_${object}";
                    my $res = main->$test_meth( $test->{description}, $meth, $op->{arguments},
                        $test->{outcome} );
                }
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
    # SERVER-5289 -- _id not taken from filter before 2.6
    if (   check_min_server_version($conn, 'v2.6.0')
        && !$coll->find_one($filter)
        && $args->{upsert}
        && exists( $args->{replacement} ) )
    {
        $outcome->{collection}{data}[-1]{_id} = ignore() if exists $outcome->{collection};
    }
    my $doc = delete $args->{replacement} || delete $args->{update};
    my $res = $coll->$method( $filter, $doc, ( scalar %$args ? $args : () ) );
    check_write_outcome( $label, $res, $outcome );
}

sub test_find_and_modify {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $filter = delete $args->{filter};
    my $doc = delete $args->{replacement} || delete $args->{update};
    $args->{returnDocument} = lc( $args->{returnDocument} )
      if exists $args->{returnDocument};
    # SERVER-17650 -- before 3.0, this case returned empty doc
    if (   check_min_server_version($conn, 'v3.0.0')
        && !$coll->find_one($filter)
        && ( !$args->{returnDocument} || $args->{returnDocument} eq 'before' )
        && $args->{upsert}
        && $args->{sort} )
    {
        $outcome->{result} = {};
    }
    # SERVER-5289 -- _id not taken from filter before 2.6
    if ( check_min_server_version($conn, 'v2.6.0') ) {
        if ( $outcome->{result}
            && ( !exists $args->{projection}{_id} || $args->{projection}{_id} ) )
        {
            $outcome->{result}{_id} = ignore();
        }

        if ( $args->{upsert} && !$coll->find_one($filter) ) {
            $outcome->{collection}{data}[-1]{_id} = ignore() if exists $outcome->{collection};
        }
    }
    my $res = $coll->$method( $filter, $doc, ( scalar %$args ? $args : () ) );
    check_find_one_outcome( $label, $res, $outcome );
}

BEGIN {
    *test_find_collection                     = \&test_read_w_filter;
    *test_count_collection                    = \&test_read_w_filter;
    *test_count_documents_collection          = \&test_read_w_filter;
    *test_estimated_document_count_collection = \&test_read_w_filter;
    *test_delete_many_collection              = \&test_write_w_filter;
    *test_delete_one_collection               = \&test_write_w_filter;
    *test_insert_many_collection              = \&test_insert;
    *test_insert_one_collection               = \&test_insert;
    *test_replace_one_collection              = \&test_modify;
    *test_update_one_collection               = \&test_modify;
    *test_update_many_collection              = \&test_modify;
    *test_find_one_and_delete_collection      = \&test_write_w_filter;
    *test_find_one_and_replace_collection     = \&test_find_and_modify;
    *test_find_one_and_update_collection      = \&test_find_and_modify;
}

#--------------------------------------------------------------------------#
# method-specific tests
#--------------------------------------------------------------------------#

sub test_bulk_write_collection {
    my ( $class, $label, $method, $args, $outcome ) = @_;

    my $bulk;

    if ( $args->{options}->{ordered} ) {
        $bulk = $coll->initialize_ordered_bulk_op;
    } else {
        $bulk = $coll->initialize_unordered_bulk_op;
    }

    for my $request ( @{ $args->{requests} } ) {
        my $req_method = $request->{name};
        my $arg = $request->{arguments};
        $req_method =~ s{([A-Z])}{_\L$1}g;
        my $filter = delete $arg->{filter};
        my $update = delete $arg->{update};
        my $arr_filters = delete $arg->{arrayFilters};
        my $bulk_view = $bulk->find( $filter );
        if ( scalar( @$arr_filters ) ) {
          $bulk_view = $bulk_view->arrayFilters( $arr_filters );
        }
        $bulk_view->$req_method( $update );
    }
    my $res = $bulk->execute;

    check_write_outcome( $label, $res, $outcome );
}

sub test_aggregate_collection {
    my ( $class, $label, $method, $args, $outcome ) = @_;

    skip_unless_min_version($conn, 'v2.2.0');

    my $pipeline = delete $args->{pipeline};

    # $out not supported until 2.6
    my $is_out = exists $pipeline->[-1]{'$out'};
    return if $is_out && check_min_server_version($conn, 'v2.6.0');

    # Perl driver returns empty result if $out
    $outcome->{result} = [] if $is_out;

    my $res = $coll->aggregate( grep { defined } $pipeline, $args );
    check_read_outcome( $label, $res, $outcome );
}

sub test_aggregate_database {
    my ( $class, $label, $method, $args, $outcome ) = @_;

    skip_unless_min_version($conn, 'v3.6.0');

    plan skip_all => "mongos mangles commands too much vs test expectations"
        if $server_type eq 'Mongos';

    my $pipeline = delete $args->{pipeline};
    my $res = $conn->get_database('admin')->aggregate($pipeline, $args);
    is($res->{'_full_name'}, 'admin.$cmd.aggregate', 'check DB aggregate full name');
    my $got = [ $res->all ]->[0]{'command'};
    my $result = $outcome->{'result'}[0]{'command'};
    $result->{'cursor'} = ignore();
    $result->{'pipeline'}[0]{'$currentOp'} = noclass(
        superhashof($result->{'pipeline'}[0]{'$currentOp'})
    );
    $result->{'pipeline'}[2]{'$project'} = ignore();
    $result->{'pipeline'}[3]{'$project'} = ignore();

    cmp_deeply(
        $got,
        noclass( superhashof($result) ),
        "$label: compare",
    ) or diag explain $got;
}

sub test_distinct_collection {
    my ( $class, $label, $method, $args, $outcome ) = @_;
    my $fieldname = delete $args->{fieldName};
    my $filter    = delete $args->{filter};
    my $res = $coll->distinct( $fieldname, $filter, $args );
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
        # Tests have upsertedCount field, but this is not required by the
        # CRUD spec itself.  It seems to be there for drivers that return
        # BulkWriteResults for everything.
        next if $k eq 'upsertedCount' && $res->isa("MongoDB::UpdateResult");
        ( my $attr = $k ) =~ s{([A-Z])}{_\L$1}g;
        if ( check_min_server_version($conn, 'v2.6.0') ) {
            $outcome->{result}{$k} = undef    if $k eq 'modifiedCount';
            $outcome->{result}{$k} = ignore() if $k eq 'upsertedId';
        }
        cmp_deeply( $res->$attr, $outcome->{result}{$k}, "$label: $k" );
    }

    check_collection( $label, $outcome );
}

sub check_find_one_outcome {
    my ( $label, $res, $outcome ) = @_;

    cmp_deeply( $res, $outcome->{result}, "$label: result doc" )
      or diag explain $res;
    check_collection( $label, $outcome );
}

sub check_insert_outcome {
    my ( $label, $res, $outcome ) = @_;

    if ( exists $outcome->{result}{insertedId} ) {
        return check_write_outcome( $label, $res, $outcome );
    }

    cmp_deeply( $res->inserted_ids , $outcome->{result}{insertedIds}, "$label: result doc" );
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
      or diag "GOT:\n", explain($data), "EXPECTED:\n",
      explain( $outcome->{collection}{data} );
}

done_testing;
