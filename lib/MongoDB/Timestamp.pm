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
package MongoDB::Timestamp;
# ABSTRACT: Replication timestamp

use version;
our $VERSION = 'v1.999.0';

use Moo;
use Types::Standard qw(
    Int
);
use namespace::clean -except => 'meta';

use overload (
    q{<=>} => \&_compare,
    fallback => 1
);

sub _compare {
    my ( $self, $target, @args ) = @_;

    my $sec_sm = $self->sec <=> $target->sec;
    if ( $sec_sm == 0 ) {
      return $self->inc <=> $target->inc;
    }
    return $sec_sm;
}


=attr sec

Seconds since epoch.

=cut

has sec => (
    is       => 'ro',
    isa      => Int,
    required => 1,
);

=attr inc

Incrementing field.

=cut

has inc => (
    is       => 'ro',
    isa      => Int,
    required => 1,
);

1;

=head1 DESCRIPTION

This is an internal type used for replication.  It is not for storing dates,
times, or timestamps in the traditional sense.  Unless you are looking to mess
with MongoDB's replication internals, the class you are probably looking for is
L<DateTime>.  See L<MongoDB::DataTypes> for more information.

=head2 Overrides

this class overrides numerical comparisons to allow for comparing two
C<MongoDB::Timestamp>s.

=cut
