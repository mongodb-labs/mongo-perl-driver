use strict;
use warnings;
use Test::More 0.88;
use Test::Fatal;

my $class = "MongoDB::WriteConcern";

require_ok( $class );

is(
    exception { $class->new },
    undef,
    "new without args has default"
);

like(
    exception { $class->new( w => 0, j => 1 ) },
    qr/can't use write concern w=0 with j=1/,
    "j=1 not allowed with w=0",
);

done_testing;

# vim: ts=4 sts=4 sw=4 et:
