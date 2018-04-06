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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    clear_testdbs
    get_unique_collection
    server_version
    server_type
    check_min_server_version
    skip_unless_mongod
    get_features
/;

skip_unless_mongod();

my $conn           = build_client(retry_writes => 1);
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $features       = get_features($conn);

plan skip_all => "retryableWrites not supported on this MongoDB"
    unless ( $features->supports_retryWrites );

plan skip_all => "mongos doesn't have failpoints needed for this tests"
    if $server_type eq 'Mongos';

sub run_test {
    my ( $coll, $test ) = @_;
    enable_failpoint( $test->{failPoint} ) if exists $test->{failPoint};

    my $op = $test->{operation};
    my $method = $op->{name};
    $method =~ s{([A-Z])}{_\L$1}g;

    my $func_name = 'do_' . $method;

    my $ret = eval { main->$func_name( $coll, $op->{arguments} ) };
    my $err = $@;

    if ( exists $test->{outcome}->{error} && $test->{outcome}->{error} ) {
        ok $err, 'Exception occured';
    }

    if ( !exists $test->{outcome}{error} && exists $test->{outcome}->{result} ) {

        #Dwarn $ret;
        #Dwarn $test->{outcome};
        for my $res_key ( keys %{ $test->{outcome}->{result} } ) {
            next if $res_key eq 'upsertedCount' && ! $ret->can('upserted_count'); # Driver does not parse this value on all things?
            # next if $res_key eq 'upsertedId' && ! defined $ret->upserted_id; # upserted id is always present
            my $res = $test->{outcome}->{result}->{$res_key};

            if ( $res_key eq 'insertedIds' ) {
                my $ret_parsed = {};
                for my $item ( @{ $ret->inserted } ) {
                  $ret_parsed->{$item->{index}} = $item->{_id};
                }
                is_deeply $ret_parsed, $test->{outcome}->{result}->{insertedIds}, 'insertedIds correct in result';
                next;
            }
            if ( $res_key eq 'upsertedIds' ) {
                my $ret_parsed = {};
                for my $item ( @{ $ret->upserted } ) {
                  $ret_parsed->{$item->{index}} = $item->{_id};
                }
                is_deeply $ret_parsed, $test->{outcome}->{result}->{upsertedIds}, 'upsertedIds correct in result';
                next;
            }
            my $ret_key = $res_key;
            $ret_key =~ s{([A-Z])}{_\L$1}g;

            is $ret->{$ret_key}, $res, "$res_key correct in result";
        }
    }

    my @coll_outcome = $coll->find()->all;
    my $coll_expected = $test->{outcome}->{collection}->{data};

    is_deeply \@coll_outcome, $coll_expected, 'Collection has correct outcome';
    disable_failpoint() if exists $test->{failPoint};
}

sub do_delete_one {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    return $coll->delete_one( $filter );
}

sub do_replace_one {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    my $replacement = defined $args->{replacement} ? $args->{replacement} : {};
    return $coll->replace_one( $filter, $replacement );
}

sub do_find_one_and_update {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    my $update = defined $args->{update} ? $args->{update} : {};
    my $options = {
        ( defined $args->{returnDocument} ? ( returnDocument => lc $args->{returnDocument} ) : () )
    };
    return $coll->find_one_and_update( $filter, $update, $options );
}

sub do_find_one_and_replace {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    my $replace = defined $args->{replacement} ? $args->{replacement} : {};
    my $options = {
        ( defined $args->{returnDocument} ? ( returnDocument => lc $args->{returnDocument} ) : () )
    };
    return $coll->find_one_and_replace( $filter, $replace, $options );
}

sub do_insert_one {
    my ( $self, $coll, $args ) = @_;
    return $coll->insert_one( $args->{document} );
}

sub do_find_one_and_delete {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    my $options = {
        ( defined $args->{sort} ? ( sort => $args->{sort} ) : () )
    };
    return $coll->find_one_and_delete( $filter, $options );
}

sub do_bulk_write {
    my ( $self, $coll, $args ) = @_;
    my $options = {
      (  defined $args->{options}
      && defined $args->{options}->{ordered}
      && $args->{options}->{ordered}
      ? ( ordered => 1 )
      : ( ordered => 0 ) )
    };

    my @arguments;
    for my $request ( @{ $args->{requests} } ) {
        if ( $request->{name} eq 'insertOne' ) {
            push @arguments, { insert_one => [ $request->{arguments}->{document} ] };
        } elsif ( $request->{name} eq 'updateOne' ) {
            push @arguments, { update_one => [
                $request->{arguments}->{filter},
                $request->{arguments}->{update},
                ( defined $request->{arguments}->{upsert}
                  ? ( { upsert => $request->{arguments}->{upsert} ? 1 : 0 } )
                  : () )
            ] };
        } elsif ( $request->{name} eq 'deleteOne' ) {
            push @arguments, { delete_one => [ $request->{arguments}->{filter} ] };
        } elsif ( $request->{name} eq 'replaceOne' ) {
            push @arguments, { replace_one => [
                $request->{arguments}->{filter},
                $request->{arguments}->{replacement}
            ] };
        }
    }
    return $coll->bulk_write( \@arguments, $options );
}

sub do_update_one {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    my $update = defined $args->{update} ? $args->{update} : {};
    my $options = {
        ( defined $args->{upsert} ? $args->{upsert} ? ( upsert => 1 ) : ( upsert => 0 ) : () )
    };
    return $coll->update_one( $filter, $update, $options );
}

sub do_insert_many {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $options = {
      (  defined $args->{options}
      && defined $args->{options}->{ordered}
      && $args->{options}->{ordered}
      ? ( ordered => 1 )
      : ( ordered => 0 ) )
    };
    return $coll->insert_many( $args->{documents}, $options );
}

my $dir      = path("t/data/retryable-writes");
my $iterator = $dir->iterator;
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }

    subtest $path => sub {
        if ( exists $plan->{minServerVersion} ) {
            my $min_version = $plan->{minServerVersion};
            plan skip_all => "Requires MongoDB $min_version"
                if check_min_server_version( $conn, $min_version );
        }

        for my $test ( @{ $plan->{tests} } ) {
            my $coll = get_unique_collection( $testdb, 'retry_write' );
            my $ret = $coll->insert_many( $plan->{data} );
            my $description = $test->{description};
            subtest $description => sub {
                run_test( $coll, $test );
            }
        }
    };
}

sub enable_failpoint {
    my $doc = shift;
    $conn->send_admin_command([
        configureFailPoint => 'onPrimaryTransactionalWrite',
        %$doc,
    ]);
}

sub disable_failpoint {
    my $doc = shift;
    $conn->send_admin_command([
        configureFailPoint => 'onPrimaryTransactionalWrite',
        mode => 'off',
    ]);
}

clear_testdbs;

done_testing;
