package Mongo::Cursor;

use Mouse;

sub next {
    my ($self) = @_;
    return unless $self->_more;
    return $self->_next;
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
