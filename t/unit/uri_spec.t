#  Copyright 2019 - present MongoDB, Inc.
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
use Test::More;
use Test::Fatal;
use Path::Tiny;
use JSON::MaybeXS;
use boolean;

my $class = "MongoDB::_URI";

require_ok($class);

my $dir      = path('t/data/uri/');
my $iterator = $dir->iterator( { recurse => 1 } );
while ( my $path = $iterator->() ) {
    next unless -f $path && $path =~ /\.json$/;
    next if $path =~ /connection-pool-options/;
    my $plan = decode_json( $path->slurp_utf8 );
    subtest $path->basename => sub {
        foreach my $test ( @{ $plan->{'tests'} } ) {
            subtest $test->{description} => sub {
                run_options_test($test);
            }
        }
    }
}

sub _booleanize_options {
    my ($options, $test) = @_;
    for my $k ( keys %$options ) {
        my $type = ref( $options->{$k} );
        # If it's a ref and not hash/array, then it must be some
        # sort of JSON boolean type, so normalize it.
        if ( $type && $type ne 'HASH' && $type ne 'ARRAY' ) {
            $options->{$k} = boolean( $options->{$k} );
            $test->{$k} = boolean( $test->{$k} );
        }
    }
}

sub run_options_test {
    my $test = shift;

    # Invalid case -- the spec claims these should never exist

    if ( ! $test->{valid} ) {
        ok( exception { $class->new( uri => $test->{uri} ) }, "should throw exception" );
        return;
    }

    # Valid case

    # maxIdleTimeMS is only for drivers with a connection pool.
    if ($test->{uri} =~ /maxIdleTimeMS/) {
        $test->{uri} =~ s{maxIdleTimeMS=\d+\&}{};
        delete $test->{options}{maxIdleTimeMS};
    }

    my @warnings = ();
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $uri;
    my $err = exception { new_ok( $class, [ uri => $test->{uri} ]) };
    is( $err, undef, "should not throw exception" );

    if ( $test->{warning} ) {
        ok( scalar @warnings, 'URI has warnings' );
    }
    else {
        ok( scalar @warnings == 0, 'URI has no warnings' );
    }

    return unless $uri;

    my $test_opts = $test->{options};
    my $uri_opts = $uri->options;
    for my $k ( map { lc } keys %$test_opts ) {
        # scalars
        if (! ref $test_opts->{$k}) {
            is( $uri_opts->{$k}, $test_opts->{$k}, $k );
        }
        else {
            fail("$k not yet tested")
        }
    }
}

done_testing;
