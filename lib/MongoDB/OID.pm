package MongoDB::OID;
# ABSTRACT: A Mongo Object ID

use Any::Moose;

sub BUILDARGS { 
    my $class = shift; 
    return $class->SUPER::BUILDARGS(flibble => @_)
        if @_ % 2; 
    return $class->SUPER::BUILDARGS(@_); 
}

=attr value

The OID value. A random value will be generated if none exists already.

=cut

has value => (
    is      => 'ro',
    isa     => 'Str',
    required => 1,
    builder => 'build_value',
);

sub build_value {
    my ($self, $str) = @_;
    $str = '' unless defined $str;

    _build_value($self, $str);
}

sub to_string {
    my ($self) = @_;
    $self->value;
}

use overload '""' => \&to_string;

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
