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

package MongoDB::UpdateResult;

# ABSTRACT: MongoDB update result object

use version;
our $VERSION = 'v0.999.998.7'; # TRIAL

use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

with 'MongoDB::Role::_WriteResult';

=attr acknowledged

Indicates whether this write result was ackowledged. If not, then all other
members of this result will be zero or undefined.

=cut

=attr matched_count

The number of documents that matched the filter.

=cut

has matched_count => (
    is      => 'ro',
    isa     => Num,
    default => 0,
);

=attr modified_count

The number of documents that were modified.  Note: this is only available
from MongoDB version 2.6 or later.  It will return C<undef> from earlier
servers.

You can call C<has_modified_count> to find out if this attribute is
defined or not.

=cut

has modified_count => (
    is      => 'ro',
    isa     => Maybe[Num],
);

sub has_modified_count {
    my ($self) = @_;
    return defined( $self->modified_count );
}

=attr upserted_id

The identifier of the inserted document if an upsert took place.  If
no upsert took place, it returns C<undef>.

=cut

has upserted_id => (
    is  => 'ro',
    isa => Any,
);

__PACKAGE__->meta->make_immutable;

1;

=method assert

Throws an error if write errors or write concern errors occurred.

=cut

=method assert_no_write_error

Throws a MongoDB::WriteError if C<count_write_errors> is non-zero; otherwise
returns 1.

=cut

=method assert_no_write_concern_error

Throws a MongoDB::WriteConcernError if C<count_write_concern_errors> is non-zero; otherwise
returns 1.

=cut


__END__

=head1 SYNOPSIS

    my $result = $coll->update( @parameters );

    if ( $result->acknowledged ) {
        ...
    }

=head1 DESCRIPTION

This class encapsulates the results from an update or replace operations.

=cut
