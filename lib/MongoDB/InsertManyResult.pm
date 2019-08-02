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
package MongoDB::InsertManyResult;

# ABSTRACT: MongoDB single insert result object

use version;
our $VERSION = 'v2.1.2';

use Moo;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    ArrayOfHashRef
    Numish
);
use Types::Standard qw(
    HashRef
);
use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_WriteResult
);

=attr inserted_count

The number of documents inserted.

=cut

has inserted_count => (
    is      => 'lazy',
    builder => '_build_inserted_count',
    isa => Numish,
);

sub _build_inserted_count
{
    my ($self) = @_;
    return scalar @{ $self->inserted };
}

=attr inserted

An array reference containing information about inserted documents (if any).
Documents are just as in C<upserted>.

=cut

has inserted => (
    is      => 'ro',
    default => sub { [] },
    isa => ArrayOfHashRef,
);

=attr inserted_ids

A hash reference built lazily from C<inserted> mapping indexes to object
IDs.

=cut

has inserted_ids => (
    is      => 'lazy',
    builder => '_build_inserted_ids',
    isa => HashRef,
);

sub _build_inserted_ids {
    my ($self) = @_;
    return { map { $_->{index}, $_->{_id} } @{ $self->inserted } };
}

1;

__END__

=method acknowledged

Indicates whether this write result was acknowledged.  Always
true for this class.

=cut

=method assert

Throws an error if write errors or write concern errors occurred.
Otherwise, returns the invocant.

=cut

=method assert_no_write_error

Throws a MongoDB::WriteError if write errors occurred.
Otherwise, returns the invocant.

=cut

=method assert_no_write_concern_error

Throws a MongoDB::WriteConcernError if write concern errors occurred.
Otherwise, returns the invocant.

=cut

=head1 SYNOPSIS

    my $result = $coll->insert( $document );

    if ( $result->acknowledged ) {
        ...
    }

=head1 DESCRIPTION

This class encapsulates the result from the insertion of a single document.

=cut
