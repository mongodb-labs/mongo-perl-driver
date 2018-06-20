#  Copyright 2013 - present MongoDB, Inc.
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

package MongoDBSpecTest;

use strict;
use warnings;

use Exporter 'import';
use Test::More;
use Path::Tiny;
use JSON::MaybeXS qw( is_bool decode_json );

our @EXPORT_OK = qw(
    foreach_spec_test
);

sub foreach_spec_test {
    my ($dir, $callback) = @_;

    $dir = path($dir);
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
                subtest $test->{description} => sub {
                    $callback->($test, $plan);
                };
            }
        };
    }
}

1;
