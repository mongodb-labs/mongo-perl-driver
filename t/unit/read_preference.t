#  Copyright 2014 - present MongoDB, Inc.
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
use Test::More 0.88;
use Test::Fatal;

my $class = "MongoDB::ReadPreference";

require_ok( $class );

is(
    exception { $class->new },
    undef,
    "new without args has default"
);

subtest "mode" => sub {
    my @modes = qw(
        primary PRIMARY PrImArY
        secondary secondary_preferred primary_preferred nearest
        secondarypreferred primarypreferred
    );

    for my $mode (@modes) {
        new_ok( $class, [ mode => $mode ], "new( mode => '$mode' )" );
    }

    like(
        exception { $class->new( mode => 'primary', tag_sets => [ { dc => 'us' } ] ) },
        qr/not allowed/,
        "tag set list not allowed with primary"
    );
};

subtest "max_staleness_seconds" => sub {
    for my $t ( -1, 90 ) {
        my $obj = new_ok(
            $class,
            [ mode => 'nearest', max_staleness_seconds => $t ],
            "new with max_staleness_seconds $t"
        );
        is( $obj->max_staleness_seconds, $t, "max_staleness_seconds is correct" );
    }

    is( $class->new->max_staleness_seconds, -1, "max_staleness_seconds default is -1" );

    like(
        exception { $class->new( mode => 'primary', max_staleness_seconds => 42 ) },
        qr/not allowed/,
        "max staleness not allowed with primary"
    );
};

subtest "stringification" => sub {
    my $rp;

    my @cases = (
        [ {} => 'primary' ],
        [ { mode => 'secondary_preferred' }, 'secondaryPreferred' ],
        [
            {
                mode    => 'secondary_preferred',
                tag_sets => [ { dc => 'ny', rack => 1 }, { dc => 'ny' }, {} ]
            },
            'secondaryPreferred ({dc:ny,rack:1},{dc:ny},{})'
        ],
    );

    for my $case (@cases) {
        my $rp = $class->new( $case->[0] );
        is( $rp->as_string, $case->[1], $case->[1] );
    }

};

done_testing;

# vim: ts=4 sts=4 sw=4 et:
