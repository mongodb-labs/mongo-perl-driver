#
#  Copyright 2014 MongoDB, Inc.
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

package MongoDB::BulkWriteView;

# ABSTRACT: Bulk write operations against a query document

use version;
our $VERSION = 'v0.704.5.1';

use Moose;
use namespace::clean -except => 'meta';

# done in two parts to work around a moose bug: https://github.com/moose/Moose/pull/19
with qw(
  MongoDB::Role::_View
  MongoDB::Role::_Writeable
);

with qw(
  MongoDB::Role::_Updater
  MongoDB::Role::_Remover
);

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 SYNOPSIS

    my $bulk = $collection->initialize_ordered_bulk_op;

    # Update one document matching the selector
    bulk->find( { a => 1 } )->update_one( { '$inc' => { x => 1 } } );

    # Update all documents matching the selector
    bulk->find( { a => 2 } )->update( { '$inc' => { x => 2 } } );

    # Update all documents
    bulk->find( {} )->update( { '$inc' => { x => 2 } } );

    # Replace entire document (update with whole doc replace)
    bulk->find( { a => 3 } )->replace_one( { x => 3 } );

    # Update one document matching the selector or upsert
    bulk->find( { a => 1 } )->upsert()->update_one( { '$inc' => { x => 1 } } );

    # Update all documents matching the selector or upsert
    bulk->find( { a => 2 } )->upsert()->update( { '$inc' => { x => 2 } } );

    # Replaces a single document matching the selector or upsert
    bulk->find( { a => 3 } )->upsert()->replace_one( { x => 3 } );

    # Remove a single document matching the selector
    bulk->find( { a => 4 } )->remove_one();

    # Remove all documents matching the selector
    bulk->find( { a => 5 } )->remove();

    # Remove all documents
    bulk->find( {} )->remove();

=head1 DESCRIPTION

This class provides means to specify write operations constrained by a query
document.

To instantiate a C<MongoDB::BulkWriteView>, use the L<find|MongoDB::BulkWrite/find>
method from L<MongoDB::BulkWrite> or the L</upsert> method described below.

Except for L</upsert>, all methods have an empty return on success; an
exception will be thrown on error.

=method remove

    $bulk->remove;

Removes all documents matching the query document.

=method remove_one

    $bulk->remove_one;

Removes a single document matching the query document.

=method replace_one

    $bulk->replace_one( $doc );

Replaces the document matching the query document.  The document
to replace must not have any keys that begin with a dollar sign, C<$>.

=method update

    $bulk->update( $modification );

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
