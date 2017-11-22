use strict;
use warnings;
package MongoDB::BSON::Regexp;
# ABSTRACT: Regular expression type

use version;
our $VERSION = 'v1.999.0';

use Moo;
use MongoDB::Error;
use Types::Standard qw(
    Str
);
use namespace::clean -except => 'meta';

=attr pattern

A string containing a regular expression pattern (without slashes)

=cut

has pattern => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

=attr flags

A string with regular expression flags

=cut

has flags => (
    is        => 'ro',
    isa       => Str,
    required  => 0,
    predicate => 'has_flags',
    writer    => '_set_flags',
);

my %ALLOWED_FLAGS = (
    i   => 1,
    m   => 1,
    x   => 1,
    l   => 1,
    s   => 1,
    u   => 1
);

sub BUILD {
    my $self = shift;

    if ( $self->has_flags ) {
        my %seen;
        my @flags = grep { !$seen{$_}++ } split '', $self->flags;
        foreach my $f( @flags ) {
            MongoDB::UsageError->throw("Regexp flag $f is not supported by MongoDB")
              if not exists $ALLOWED_FLAGS{$f};
        }

        $self->_set_flags( join '', sort @flags );
    }
}

=method try_compile

    my $qr = $regexp->try_compile;

Tries to compile the C<pattern> and C<flags> into a reference to a regular
expression.  If the pattern or flags can't be compiled, a
C<MongoDB::DecodingError> exception will be thrown.

B<SECURITY NOTE>: Executing a regular expression can evaluate arbitrary
code.  You are strongly advised never to use untrusted input with
C<try_compile>.

=cut

sub try_compile {
    my ($self) = @_;
    my ( $p, $f ) = map { $self->$_ } qw/pattern flags/;
    my $re = eval { qr/(?$f:$p)/ };
    MongoDB::DecodingError->throw("error compiling regex 'qr/$p/$f': $@")
      if $@;
    return $re;
}


1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
