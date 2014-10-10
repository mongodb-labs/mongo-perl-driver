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

my @modes = qw(
    primary PRIMARY PrImArY
    secondary secondary_preferred primary_preferred nearest
    secondarypreferred primarypreferred
);

for my $mode (@modes) {
    new_ok( $class, [ mode => $mode ], "new( mode => '$mode' )" );
}

like(
    exception { $class->new( mode => 'primary', tagsets => [ { dc => 'us' } ] ) },
    qr/not allowed/,
    "tags not allowed with primary"
);

subtest "stringification" => sub {
    my $rp;

    my @cases = (
        [ {} => 'primary' ],
        [ { mode => 'secondary_preferred' }, 'secondaryPreferred' ],
        [
            {
                mode    => 'secondary_preferred',
                tagsets => [ { dc => 'ny', rack => 1 }, { dc => 'ny' }, {} ]
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
