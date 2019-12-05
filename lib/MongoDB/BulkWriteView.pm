#  Copyright 2014 - present MongoDB, Inc.
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

use strict;
use warnings;
package MongoDB::BulkWriteView;

# ABSTRACT: Bulk write operations against a query document

use version;
our $VERSION = 'v2.2.1';

use Moo;

use MongoDB::Error;
use MongoDB::_Types qw(
    Boolish
    Document
    IxHash
);
use Types::Standard qw(
    Maybe
    ArrayRef
    InstanceOf
);
use boolean;
use namespace::clean -except => 'meta';

# A hash reference containing a MongoDB query document
has _query => (
    is       => 'ro',
    isa      => IxHash,
    coerce   => IxHash->coercion,
    required => 1
);

# Originating bulk write object for executing write operations.
has _bulk => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::BulkWrite'],
    required => 1,
    handles  => [qw/_enqueue_write/]
);

has _collation => (
    is  => 'ro',
    isa => Maybe [Document],
);

has _array_filters => (
    is => 'ro',
    isa => Maybe [ArrayRef[Document]],
);

has _upsert => (
    is      => 'ro',
    isa     => Boolish,
    default => 0,
);

sub collation {
    my ($self, $collation) = @_;
    return $self->new( %$self, _collation => $collation );
}

sub arrayFilters {
  my ( $self, $array_filters ) = @_;
  return $self->new( %$self, _array_filters => $array_filters );
}

sub upsert {
    my ($self) = @_;
    unless ( @_ == 1 ) {
        MongoDB::UsageError->throw("the upsert method takes no arguments");
    }
    return $self->new( %$self, _upsert => true );
}

sub update_many {
    push @_, "update_many";
    goto &_update;
}

sub update_one {
    push @_, "update_one";
    goto &_update;
}

sub replace_one {
    push @_, "replace_one";
    goto &_update;
}

sub _update {
    my $method = pop @_;
    my ( $self, $doc ) = @_;

    my $type = ref $doc;
    unless ( @_ == 2 && grep { $type eq $_ } qw/HASH ARRAY Tie::IxHash BSON::Array/ ) {
        MongoDB::UsageError->throw("argument to $method must be a single hashref, arrayref, Tie::IxHash or BSON::Array");
    }

    if ( ref $doc eq 'ARRAY' ) {
        MongoDB::UsageError->throw("array reference to $method must have key/value pairs")
          if @$doc % 2;
        $doc = Tie::IxHash->new(@$doc);
    }
    elsif ( ref $doc eq 'HASH' ) {
        $doc = Tie::IxHash->new(%$doc);
    }

    $self->_bulk->_retryable( 0 ) if $method eq 'update_many';

    my $update = {
        q      => $self->_query,
        u      => $doc,
        multi  => $method eq 'update_many' ? true : false,
        upsert => boolean( $self->_upsert ),
        is_replace => $method eq 'replace_one',
        (defined $self->_collation ? (collation => $self->_collation) : ()),
        (defined $self->_array_filters ? (arrayFilters => $self->_array_filters) : ()),
    };

    $self->_enqueue_write( [ update => $update ] );

    return;
}

sub delete_many {
    my ($self) = @_;
    $self->_bulk->_retryable( 0 );
    $self->_enqueue_write(
        [
            delete => {
                q     => $self->_query,
                limit => 0,
                ( defined $self->_collation ? ( collation => $self->_collation ) : () ),
                (defined $self->_array_filters ? (arrayFilters => $self->_array_filters) : ()),
            }
        ]
    );
    return;
}

sub delete_one {
    my ($self) = @_;
    $self->_enqueue_write(
        [
            delete => {
                q     => $self->_query,
                limit => 1,
                ( defined $self->_collation ? ( collation => $self->_collation ) : () ),
                (defined $self->_array_filters ? (arrayFilters => $self->_array_filters) : ()),
            }
        ]
    );
    return;
}

1;

__END__

=head1 SYNOPSIS

    my $bulk = $collection->initialize_ordered_bulk_op;

    # Update one document matching the selector
    bulk->find( { a => 1 } )->update_one( { '$inc' => { x => 1 } } );

    # Update all documents matching the selector
    bulk->find( { a => 2 } )->update_many( { '$inc' => { x => 2 } } );

    # Update all documents matching the selector, with respect to a collation
    bulk->find( { a => { '$gte' => 'F' } )->collation($collation)
          ->update_many( { '$inc' => { x => 2 } } );

    # Update all documents
    bulk->find( {} )->update_many( { '$inc' => { x => 2 } } );

    # Replace entire document (update with whole doc replace)
    bulk->find( { a => 3 } )->replace_one( { x => 3 } );

    # Update one document matching the selector or upsert
    bulk->find( { a => 1 } )->upsert()->update_one( { '$inc' => { x => 1 } } );

    # Update all documents matching the selector or upsert
    bulk->find( { a => 2 } )->upsert()->update_many( { '$inc' => { x => 2 } } );

    # Replaces a single document matching the selector or upsert
    bulk->find( { a => 3 } )->upsert()->replace_one( { x => 3 } );

    # Remove a single document matching the selector
    bulk->find( { a => 4 } )->delete_one();

    # Remove all documents matching the selector
    bulk->find( { a => 5 } )->delete_many();

    # Update any arrays with the matching filter
    bulk->find( {} )->arrayFilters([ { 'i.b' => 1 } ])->update_many( { '$set' => { 'y.$[i].b' => 2 } } );

    # Remove all documents matching the selector, with respect to a collation
    bulk->find( { a => { '$gte' => 'F' } )->collation($collation)->delete_many();

    # Remove all documents
    bulk->find( {} )->delete_many();

=head1 DESCRIPTION

This class provides means to specify write operations constrained by a query
document.

To instantiate a C<MongoDB::BulkWriteView>, use the L<find|MongoDB::BulkWrite/find>
method from L<MongoDB::BulkWrite>.

Except for L</arrayFilters>, L</collation> and L</upsert>, all methods have an
empty return on success; an exception will be thrown on error.

=method arrayFilters

    $bulk->arrayFilters( $array_filters )->update_many( $modification );

Returns a new C<MongoDB::BulkWriteView> object, where the specified arrayFilter
will be used to determine which array elements to modify for an update
operation on an array field.

=method collation

    $bulk->collation( $collation )->delete_one;

Returns a new C<MongoDB::BulkWriteView> object, where the specified
collation will be used to determine which documents match the query
document.  A collation can be specified for any deletion, replacement,
or update.

=method delete_many

    $bulk->delete_many;

Removes all documents matching the query document.

=method delete_one

    $bulk->delete_one;

Removes a single document matching the query document.

=method replace_one

    $bulk->replace_one( $doc );

Replaces the document matching the query document.  The document
to replace must not have any keys that begin with a dollar sign, C<$>.

=method update_many

    $bulk->update_many( $modification );

Updates all documents  matching the query document.  The modification
document must have all its keys begin with a dollar sign, C<$>.

=method update_one

    $bulk->update_one( $modification );

Updates a single document matching the query document.  The modification
document must have all its keys begin with a dollar sign, C<$>.

=method upsert

    $bulk->upsert->replace_one( $doc );

Returns a new C<MongoDB::BulkWriteView> object that will treat every
update, update_one or replace_one operation as an upsert operation.

=cut
