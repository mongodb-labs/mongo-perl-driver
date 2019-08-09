#  Copyright 2018 - present MongoDB, Inc.
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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Deep ':v1';
use Safe::Isa;

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
    skip_unless_sessions
    skip_unless_failpoints_available
    to_snake_case
    remap_hashref_to_snake_case
    get_features
    set_failpoint
    clear_failpoint
/;
use MongoDBSpecTest qw(foreach_spec_test maybe_skip_multiple_mongos);

skip_unless_mongod();
skip_unless_sessions();
skip_unless_failpoints_available( { skip_mongos => 1 });

my $conn           = build_client();
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);

sub run_test {
    my ( $coll, $test ) = @_;
    set_failpoint( $conn, $test->{failPoint} );

    my $op = $test->{operation};
    my $method = $op->{name};
    $method =~ s{([A-Z])}{_\L$1}g;

    my $func_name = 'do_' . $method;

    my $ret = eval { main->$func_name( $coll, $op->{arguments} ) };
    my $err = $@;

    if ( exists $test->{outcome}->{error} && $test->{outcome}->{error} ) {
        ok $err, 'Exception occured';
    }
    else {
        is( $err, "", "No exception occured" );
    }

    if ( !exists $test->{outcome}{error} && exists $test->{outcome}->{result} ) {

        my $expected = remap_hashref_to_snake_case( $test->{outcome}->{result} );
        # not all commands return an upserted count
        delete $expected->{upserted_count} unless $ret->$_can('upserted_count');

        for my $key ( keys %$expected ) {
            my $got = ref $ret eq 'HASH' ? $ret->{$key} : $ret->$key;
            cmp_deeply $got, $expected->{$key}, "$key result as expected";
        }
    }

    my @coll_outcome = $coll->find()->all;
    my $coll_expected = $test->{outcome}->{collection}->{data};

    is_deeply \@coll_outcome, $coll_expected, 'Collection has correct outcome';
    clear_failpoint( $conn, $test->{failPoint} );
}

sub do_delete_one {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    return $coll->delete_one( $filter );
}

sub do_delete_many {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    return $coll->delete_many( $filter );
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

my %bulk_remap = (
    insert_one  => [qw( document )],
    update_one  => [qw( filter update )],
    update_many => [qw( filter update )],
    replace_one => [qw( filter replacement )],
    delete_one  => [qw( filter )],
    delete_many => [qw( filter )],
);

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
        my $req_name = to_snake_case( $request->{name} );
        my @req_fields = @{ $bulk_remap{ $req_name } };
        my @arg = map {
            delete $request->{arguments}->{ $_ }
        } @req_fields;
        push @arg, $request->{arguments} if keys %{ $request->{arguments} };
        push @arguments, { $req_name => \@arg };
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

sub do_update_many {
    my ( $self, $coll, $args ) = @_;
    $args //= {};
    my $filter = defined $args->{filter} ? $args->{filter} : {};
    my $update = defined $args->{update} ? $args->{update} : {};
    my $options = {
        ( defined $args->{upsert} ? $args->{upsert} ? ( upsert => 1 ) : ( upsert => 0 ) : () )
    };
    return $coll->update_many( $filter, $update, $options );
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

foreach_spec_test("t/data/retryable-writes", $conn, sub {
    my ($test, $plan) = @_;
    my $client_options = $test->{clientOptions};
    $client_options = remap_hashref_to_snake_case( $client_options );
    my $test_conn = build_client( %$client_options );
    my $test_db = get_test_db( $test_conn );
    my $coll = get_unique_collection( $test_db, 'retry_write' );
    my $ret = $coll->insert_many( $plan->{data} );
    my $description = $test->{description};

    subtest $description => sub {
        maybe_skip_multiple_mongos( $conn, $test->{useMultipleMongoses} );
	run_test( $coll, $test );
    }
});

clear_testdbs;

done_testing;
