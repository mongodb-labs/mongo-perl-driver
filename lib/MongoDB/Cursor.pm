package MongoDB::Cursor;
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
    default  => 'MongoDB::OID',
);


has _queried => (
    is       => 'rw',
    isa      => 'Bool',
    required => 1,
    default  => 0,
);


=method sort

    # sort by name, descending
    my $sort = {"name" => -1};
    $cursor = $coll->find->sort($sort);

Adds a sort to the query.
Returns this cursor for chaining operations.


=method snapshot

    my $cursor = $coll->find->snapshot;

Uses snapshot mode for the query.  Snapshot mode assures no 
duplicates are returned, or objects missed, which were present 
at both the start and end of the query's execution (if an object 
is new during the query, or deleted during the query, it may or 
may not be returned, even with snapshot mode).  Note that short 
query responses (less than 1MB) are always effectively 
snapshotted.  Currently, snapshot mode may not be used with 
sorting or explicit hints.

=method has_next

    while ($cursor->has_next) {
        ...
    }

Checks if there is another result to fetch.


=method next

    while (my $object = $cursor->next) {
        ...
    }

Returns the next object in the cursor. Will automatically fetch more data from
the server if necessary. Returns undef if no more data is available.


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
