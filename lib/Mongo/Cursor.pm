package Mongo::Cursor;
# ABSTRACT: A cursor/iterator for Mongo query results

use Any::Moose;

=head1 SYNOPSIS

    while (my $object = $cursor->next) {
        ...
    }

    my @objects = $cursor->all;

=cut

has _oid_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'Mongo::OID',
);

=method next

    while (my $object = $cursor->next) {
        ...
    }

Returns the next object in the cursor. Will automatically fetch more data from
the server if necessary. Returns undef if no more data is available.

=cut

sub next {
    my ($self) = @_;
    return unless $self->_more;
    return $self->_next;
}

=method all

    my @objects = $cursor->all;

Returns a list of all objects in the result.

=cut

sub all {
    my ($self) = @_;
    my @ret;

    while (my $entry = $self->next) {
        push @ret, $entry;
    }

    return @ret;
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
