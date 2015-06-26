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

package MongoDB::DBRef;

# ABSTRACT: A MongoDB database reference

use version;
our $VERSION = 'v0.999.999.3'; # TRIAL

use Tie::IxHash;
use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

# no type constraint since an _id can be anything
has id => (
    is        => 'ro',
    required  => 1
);

has ref => (
    is        => 'ro',
    isa       => DBRefColl,
    required  => 1,
    coerce    => 1,
);

has db => (
    is        => 'ro',
    isa       => Maybe[DBRefDB],
    coerce    => 1,
);

sub _ordered {
    my $self = shift;

    return Tie::IxHash->new(
        '$ref' => $self->ref,
        '$id'  => $self->id,
        ( defined($self->db) ? ( '$db' => $self->db ) : () )
    );
}

__PACKAGE__->meta->make_immutable;

1;


__END__

=head1 SYNOPSIS

    my $dbref = MongoDB::DBRef->new(
        ref => 'my_collection',
        id => 123
    );

    $coll->insert( { foo => 'bar', other_doc => $dbref } );

=head1 DESCRIPTION

This module provides support for database references (DBRefs) in the Perl
MongoDB driver. A DBRef is a special embedded document which points to
another document in the database. DBRefs are not the same as foreign keys
and do not provide any referential integrity or constraint checking. For example,
a DBRef may point to a document that no longer exists (or never existed.)

=head1 ATTRIBUTES

=head2 db

Required. The database in which the referenced document lives. Either a L<MongoDB::Database>
object or a string containing the database name. The object will be coerced to string form.

=head2 ref

Required. The collection in which the referenced document lives. Either a L<MongoDB::Collection>
object or a string containing the collection name. The object will be coerced to string form.

=head2 id

Required. The C<_id> value of the referenced document. If the
C<_id> is an ObjectID, then you must use a L<MongoDB::OID> object.

=head1 METHODS


