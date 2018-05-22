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

use strict;
use warnings;
package MongoDB::BulkWrite;

# ABSTRACT: MongoDB bulk write interface

use version;
our $VERSION = 'v1.8.3';

use MongoDB::Error;
use MongoDB::Op::_BulkWrite;
use MongoDB::BulkWriteResult;
use MongoDB::BulkWriteView;

use Moo;
use MongoDB::_Types qw(
    Boolish
    to_WriteConcern
);
use Types::Standard qw(
    ArrayRef
    InstanceOf
);
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
    isa      => Boolish,
    required => 1,
);

=attr bypassDocumentValidation

A boolean for whether or not operations should bypass document validation.
Default is false.

=cut

has 'bypassDocumentValidation' => (
    is       => 'ro',
    isa      => Boolish,
);

has '_executed' => (
    is       => 'rw',
    isa      => Boolish,
    init_arg => undef,
    default  => 0,
);

has '_queue' => (
    is       => 'rw',
    isa      => ArrayRef[ArrayRef],
    init_arg => undef,
    default  => sub { [] },
);

sub _enqueue_write {
    my $self = shift;
    push @{$self->{_queue}}, @_;
}

sub _all_writes { return @{$_[0]->{_queue}} }

sub _count_writes { return scalar @{$_[0]->{_queue}} }

sub _clear_writes { @{$_[0]->{_queue}} = (); return; }

has '_database' => (
    is         => 'lazy',
    isa        => InstanceOf['MongoDB::Database'],
);

sub _build__database {
    my ($self) = @_;
    return $self->collection->database;
}

has '_client' => (
    is         => 'lazy',
    isa        => InstanceOf['MongoDB::MongoClient'],
);

sub _build__client {
    my ($self) = @_;
    return $self->_database->_client;
}

with $_ for qw(
  MongoDB::Role::_DeprecationWarner
);

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

    MongoDB::UsageError->throw("find requires a criteria document. Use an empty hashref for no criteria.")
      unless defined $doc;

    my $type = ref $doc;
    unless ( @_ == 2 && grep { $type eq $_ } qw/HASH ARRAY Tie::IxHash/ ) {
        MongoDB::UsageError->throw("argument to find must be a single hashref, arrayref or Tie::IxHash");
    }

    if ( ref $doc eq 'ARRAY' ) {
        MongoDB::UsageError->throw("array reference to find must have key/value pairs")
          if @$doc % 2;
        $doc = {@$doc};
    }

    return MongoDB::BulkWriteView->new(
        _query => $doc,
        _bulk  => $self,
    );
}

=method insert_one

    $bulk->insert_one( $doc );

Queues a document for insertion when L</execute> is called.  The document may
be a hash reference, an array reference (with balanced key/value pairs) or a
L<Tie::IxHash> object.  If the document does not have an C<_id> field, one will
be added to the original.

The method has an empty return on success; an exception will be thrown on error.

=cut

sub insert_one {
    MongoDB::UsageError->throw("insert_one requires a single document reference as an argument")
      unless @_ == 2 && ref( $_[1] );

    my ( $self, $doc ) = @_;

    if ( ref $doc eq 'ARRAY' ) {
        MongoDB::UsageError->throw("array reference to find must have key/value pairs")
          if @$doc % 2;
        $doc = {@$doc};
    }

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
    my ( $self, $write_concern  ) = @_;
    $write_concern = to_WriteConcern($write_concern)
        if defined($write_concern) && ref($write_concern) ne 'MongoDB::WriteConcern';

    if ( $self->_executed ) {
        MongoDB::UsageError->throw("bulk op execute called more than once");
    }
    else {
        $self->_executed(1);
    }

    unless ( $self->_count_writes ) {
        MongoDB::UsageError->throw("no bulk ops to execute");
    }

    $write_concern ||= $self->collection->write_concern;


    my $op = MongoDB::Op::_BulkWrite->_new(
        db_name                  => $self->_database->name,
        coll_name                => $self->collection->name,
        full_name                => $self->collection->full_name,
        queue                    => $self->_queue,
        ordered                  => $self->ordered,
        bypassDocumentValidation => $self->bypassDocumentValidation,
        bson_codec               => $self->collection->bson_codec,
        write_concern            => $write_concern,
    );

    return $self->_client->send_write_op( $op );
}

#--------------------------------------------------------------------------#
# Deprecated methods
#--------------------------------------------------------------------------#

sub insert {
    my $self = shift;

    $self->_warn_deprecated( 'insert' => [qw/insert_one/] );

    return $self->insert_one(@_);
}

1;

__END__

=head1 SYNOPSIS

    use Safe::Isa;
    use Try::Tiny;

    my $bulk = $collection->initialize_ordered_bulk_op;

    $bulk->insert_one( $doc );
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
