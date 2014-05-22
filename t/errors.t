use strict;
use warnings;
use Test::More 0.88;
use Test::Fatal;

use MongoDB::Error;
use MongoDB::WriteResult;

# check if FIRST->throw give object that isa SECOND
my @isa_checks = qw(
  MongoDB::Error              MongoDB::Error
  MongoDB::ConnectionError    MongoDB::Error
);

while (@isa_checks) {
    my ( $error, $isa ) = splice( @isa_checks, 0, 2 );
    isa_ok( exception { $error->throw }, $isa );
}

my $result = MongoDB::WriteResult->new( nModified => 0 );
my $error = exception {
    MongoDB::WriteError->throw(
        message => "whoops",
        result => $result,
    );
};

isa_ok( $error, 'MongoDB::DatabaseError', "MongoDB::WriteError" );
isa_ok( $error, 'MongoDB::Error',         "MongoDB::WriteError" );
is( $error->message, "whoops", "object message captured" );
is_deeply( $error->result, $result, "object details captured" );
is( "$error", "whoops", "object stringifies to error message" );

done_testing;

# vim: ts=4 sts=4 sw=4 et:
