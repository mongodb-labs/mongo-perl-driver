#
#  Copyright 2009-2013 MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

package MongoDB::BulkWrite;

# ABSTRACT: MongoDB bulk write interface

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use MongoDB::Error;
use MongoDB::OID;
use MongoDB::Op::_BulkWrite;
use MongoDB::BulkWriteResult;
use MongoDB::BulkWriteView;
use Syntax::Keyword::Junction qw/any/;

use Moose;
use Types::Standard -types;
use namespace::clean -except => 'meta';

=attr collection (required)

The L<MongoDB::Collection> where the operations are to be performed.

=cut

has 'collection' => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::Collection'],
    required => 1,
);

=attr ordered (required)

A boolean for whether or not operations should be ordered (true) or
unordered (false).

=cut

has 'ordered' => (
    is       => 'ro',
    isa      => Bool,
    required => 1,
);

has '_executed' => (
    is       => 'rw',
    isa      => Bool,
    init_arg => undef,
    default  => 0,
);

has '_queue' => (
    is       => 'rw',
    isa      => ArrayRef[ArrayRef],
    init_arg => undef,
    default  => sub { [] },
    traits   => ['Array'],
    handles  => {
        _enqueue_write => 'push',
        _all_writes    => 'elements',
        _count_writes  => 'count',
        _clear_writes  => 'clear',
    }
);

has '_database' => (
    is         => 'ro',
    isa        => InstanceOf['MongoDB::Database'],
    lazy_build => 1,
);

sub _build__database {
    my ($self) = @_;
    return $self->collection->_database;
}

has '_client' => (
    is         => 'ro',
    isa        => InstanceOf['MongoDB::MongoClient'],
    lazy_build => 1,
);

sub _build__client {
    my ($self) = @_;
    return $self->_database->_client;
}

=method find

    $view = $bulk->find( $query_document );

The C<find> method returns a L<MongoDB::BulkWriteView> object that allows
write operations like C<update> or C<remove>, constrained by a query document.

A query document is required.  Use an empty hashref for no criteria:

    $bulk->find( {} )->remove; # remove all documents!

An exception will be thrown on error.

=cut

sub find {
    my ( $self, $doc ) = @_;

    confess "find requires a criteria document. Use an empty hashref for no criteria."
      unless defined $doc;

    unless ( @_ == 2 && ref $doc eq any(qw/HASH ARRAY Tie::IxHash/) ) {
        confess "argument to find must be a single hashref, arrayref or Tie::IxHash";
    }

    if ( ref $doc eq 'ARRAY' ) {
        confess "array reference to find must have key/value pairs"
          if @$doc % 2;
        $doc = {@$doc};
    }

    return MongoDB::BulkWriteView->new(
        _query => $doc,
        _bulk  => $self,
    );
}

=method insert

    $bulk->insert( $doc );

Queues a document for insertion when L</execute> is called.  The document may
be a hash reference, an array reference (with balance key/value pairs) or a
L<Tie::IxHash> object.  If the document does not have an C<_id> field, one will
be added to the original.

The method has an empty return on success; an exception will be thrown on error.

=cut

sub insert {
    my ( $self, $doc ) = @_;

    unless ( @_ == 2 && ref $doc eq any(qw/HASH ARRAY Tie::IxHash/) ) {
        confess "argument to insert must be a single hashref, arrayref or Tie::IxHash";
    }

    if ( ref $doc eq 'ARRAY' ) {
        confess "array reference to insert must have key/value pairs"
          if @$doc % 2;
        $doc = Tie::IxHash->new(@$doc);
    }

    $self->collection->_add_oids([$doc]);
    $self->_enqueue_write( [ insert => $doc ] );
    return;
}

=method execute

    my $result = $bulk->execute;

Executes the queued operations.  The order and semantics depend on
whether the bulk object is ordered or unordered:

=for :list
* ordered — operations are executed in order, but operations of the same type
  (e.g. multiple inserts) may be grouped together and sent to the server.  If
  the server returns an error, the bulk operation will stop and an error will
  be thrown.
* unordered — operations are grouped by type and sent to the server in an
  unpredictable order.  After all operations are sent, if any errors occurred,
  an error will be thrown.

When grouping operations of a type, operations will be sent to the server in
batches not exceeding 16MiB or 1000 items (for a version 2.6 or later server)
or individually (for legacy servers without write command support).

This method returns a L<MongoDB::BulkWriteResult> object if the bulk operation
executes successfully.

Typical errors might include:

=for :list
* C<MongoDB::WriteError> — one or more write operations failed
* C<MongoDB::WriteConcernError> - all writes were accepted by a primary, but
  the write concern failed
* C<MongoDB::DatabaseError> — a command to the database failed entirely

See L<MongoDB::Error> for more on error handling.

B<NOTE>: it is an error to call C<execute> without any operations or
to call C<execute> more than once on the same bulk object.

=cut

sub execute {
    my ( $self, $write_concern ) = @_;
    if ( $self->_executed ) {
        MongoDB::Error->throw("bulk op execute called more than once");
    }
    else {
        $self->_executed(1);
    }

    unless ( $self->_count_writes ) {
        MongoDB::Error->throw("no bulk ops to execute");
    }

    $write_concern ||= $self->collection->write_concern;

    my $op = MongoDB::Op::_BulkWrite->new(
        db_name       => $self->_database->name,
        coll_name     => $self->collection->name,
        queue         => $self->_queue,
        ordered       => $self->ordered,
        write_concern => $write_concern,
    );

    return $self->_client->send_write_op( $op );
}


__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 SYNOPSIS

    use Safe::Isa;
    use Try::Tiny;

    my $bulk = $collection->initialize_ordered_bulk_op;

    $bulk->insert( $doc );
    $bulk->find( $query )->upsert->replace_one( $doc )
    $bulk->find( $query )->update( $modification )

    my $result = try {
        $bulk->execute;
    }
    catch {
        if ( $_->$isa("MongoDB::WriteConcernError") ) {
            warn "Write concern failed";
        }
        else {
            die $_;
        }
    };

=head1 DESCRIPTION

This class constructs a list of write operations to perform in bulk for a
single collection.  On a MongoDB 2.6 or later server with write command support
this allow grouping similar operations together for transit to the database,
minimizing network round-trips.

To begin a bulk operation, use one these methods from L<MongoDB::Collection>:

=for :list
* L<initialize_ordered_bulk_op|MongoDB::Collection/initialize_ordered_bulk_op>
* L<initialize_unordered_bulk_op|MongoDB::Collection/initialize_unordered_bulk_op>

=head2 Ordered Operations

With an ordered operations list, MongoDB executes the write operations in the
list serially. If an error occurs during the processing of one of the write
operations, MongoDB will return without processing any remaining write
operations in the list.

=head2 Unordered Operations

With an unordered operations list, MongoDB can execute in parallel, as well as
in a nondeterministic order, the write operations in the list. If an error
occurs during the processing of one of the write operations, MongoDB will
continue to process remaining write operations in the list.

=cut
