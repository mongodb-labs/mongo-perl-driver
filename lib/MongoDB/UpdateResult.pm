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
our $VERSION = 'v1.4.2';

use Moo;
use MongoDB::_Constants;
use Types::Standard qw(
    Num
    Undef
);
use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_WriteResult
);

=attr matched_count

The number of documents that matched the filter.

=cut

has matched_count => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

=attr modified_count

The number of documents that were modified.  Note: this is only available
from MongoDB version 2.6 or later.  It will return C<undef> from earlier
servers.

You can call C<has_modified_count> to find out if this attribute is
defined or not.

=cut

has modified_count => (
    is       => 'ro',
    required => 1,
    isa      => (Num|Undef),
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
);

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

    my $result = $coll->update( @parameters );

    if ( $result->acknowledged ) {
        ...
    }

=head1 DESCRIPTION

This class encapsulates the results from an update or replace operations.

=cut
