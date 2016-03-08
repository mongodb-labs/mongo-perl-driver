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

package MongoDB::DeleteResult;

# ABSTRACT: MongoDB deletion result object

use version;
our $VERSION = 'v1.2.4';

use Moo;
use MongoDB::_Constants;
use Types::Standard qw(
    Num
);
use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_WriteResult
);

=attr deleted_count

The number of documents that matched the filter.

=cut

has deleted_count => (
    is      => 'ro',
    default => 0,
    isa => Num,
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

    my $result = $coll->delete( { _id => $oid } );

    if ( $result->acknowledged ) {
        ...
    }

=head1 DESCRIPTION

This class encapsulates the results from a deletion operation.

=cut
