package Mongo::Cursor;

use Mouse;

has _oid_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'Mongo::OID',
);

sub next {
    my ($self) = @_;
    return unless $self->_more;
    return $self->_next;
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
