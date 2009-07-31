package MongoDB::Collection;
# ABSTRACT: A Mongo Collection

use Any::Moose;

has _database => (
    is       => 'ro',
    isa      => 'MongoDB::Database',
    required => 1,
    handles  => [qw/query find_one insert update remove ensure_index/],
);

=attr name

The name of the collection.

=cut

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

=attr full_name

The full_name of the collection, including the namespace of the database it's
in.

=cut

has full_name => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_full_name',
);

sub _build_full_name {
    my ($self) = @_;
    my $name    = $self->name;
    my $db_name = $self->_database->name;
    return "${db_name}.${name}";
}

=method query ($query, \%attrs?)

    my $cursor = $collection->query({ i => { '$gt' => 42 } });

    my $cursor = $collection->query({ }, { limit => 10, skip => 10 });

    my $cursor = $collection->query(
        { location => "Vancouver" },
        { sort_by  => { age => 1 } },
    );

Executes the given C<$query> and returns a C<MongoDB::Cursor> with the results.
A hash reference of attributes may be passed as the second argument.

Valid query attributes are:

=over 4

=item limit

Limit the number of results.

=item skip

Skip a number of results.

=item sort_by

Order results.

=back

=method find_one ($query)

    my $object = $collection->find_one({ name => 'Resi' });

Executes the given C<$query> and returns the first object matching it.

=method insert ($object)

    my $id = $collection->insert({ name => 'mongo', type => 'database' });

Inserts the given C<$object> into the database and returns it's id
value. The id is the C<_id> value specified in the data or a C<MongoDB::OID>.

=method update ($update, $object, $upsert?)

    $collection->update($object);

Updates an existing C<$object> in the database.

=method remove ($query)

    $collection->remove({ answer => { '$ne' => 42 } });

Removes all objects matching the given C<$query> from the database.

=method ensure_index (\@keys, $direction?, $unique?)

    $collection->ensure_index([qw/foo bar/]);

Makes sure the given C<@keys> of this collection are indexed. The optional
index direction defaults to C<ascending>.

=cut

around qw/query find_one insert update remove ensure_index/ => sub {
    my ($next, $self, @args) = @_;
    return $self->$next($self->_query_ns, @args);
};

sub _query_ns {
    my ($self) = @_;
    return $self->name;
}

=method count ($query)

    my $n_objects = $collection->count({ name => 'Bob' });

Counts the number of objects in this collection that match the given C<$query>.

=cut

sub count {
    my ($self, $query) = @_;
    $query ||= {};

    my $obj;
    eval {
        $obj = $self->_database->run_command({
            count => $self->name,
            query => $query,
        });
    };

    if ($obj =~ m/^ns missing/) {
        return 0;
    }

    return $obj->{n};
}

=method validate

    $collection->validate;

Asks the server to validate this collection.

=cut

sub validate {
    my ($self, $scan_data) = @_;
    $scan_data = 0 unless defined $scan_data;
    my $obj = $self->_database->run_command({ validate => $self->name });
}

=method drop_indexes

    $collection->drop_indexes;

Removes all indexes from this collection.

=cut

sub drop_indexes {
    my ($self) = @_;
    return $self->drop_index('*');
}

=method drop_index ($index_name)

    $collection->drop_index('foo');

Removes an index called C<$index_name> from this collection.

=cut

sub drop_index {
    my ($self, $index_name) = @_;
    return $self->_database->run_command([
        deleteIndexes => $self->name,
        index         => $index_name,
    ]);
}

=method get_indexes

    my @indexes = $collection->get_indexes;

Returns a list of all indexes of this collection.

=cut

sub get_indexes {
    my ($self) = @_;
    return $self->_database->get_collection('system.indexes')->query({
        ns => $self->full_name,
    })->all;
}

=method drop

    $collection->drop;

Deletes a collection as well as all of its indexes.

=cut

sub drop {
    my ($self) = @_;
    $self->_database->run_command({ drop => $self->name });
    return;
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
