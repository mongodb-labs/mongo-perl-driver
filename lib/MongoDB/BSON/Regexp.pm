package MongoDB::BSON::Regexp;
# ABSTRACT: Regular expression type

use version;
our $VERSION = 'v0.703.5'; # TRIAL

use Moose;
use namespace::clean -except => 'meta';

has pattern => ( 
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has flags => ( 
    is        => 'ro',
    isa       => 'Str',
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
            die "Regexp flag $f is not supported by MongoDB" if not exists $ALLOWED_FLAGS{$f};
        }

        $self->_set_flags( join '', sort @flags );
    }
}

__PACKAGE__->meta->make_immutable;

1;
