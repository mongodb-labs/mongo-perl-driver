package MongoDB::BSON::Regexp;
# ABSTRACT: Regular expression type

use version;
our $VERSION = 'v0.999.998.6';

use Moose;
use MongoDB::Error;
use Types::Standard -types;
use namespace::clean -except => 'meta';

# XXX needs overloading for =~ and qr
use overload
    'qr' => sub {
        my ($p,$f) = map { $_[0]->$_ } qw/pattern flags/;
        eval "qr/$p/$f";
    },
    fallback => 1;

has pattern => ( 
    is       => 'ro',
    isa      => Str,
    required => 1,
);

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

__PACKAGE__->meta->make_immutable;

1;
