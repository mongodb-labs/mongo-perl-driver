#  Copyright 2015 - present MongoDB, Inc.
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
package MongoDB::ReadConcern;

# ABSTRACT: Encapsulate and validate a read concern

use version;
our $VERSION = 'v1.999.1';

use Moo;
use MongoDB::Error;
use Types::Standard qw(
    Maybe
    Str
    ArrayRef
);

use namespace::clean;

=attr level

The read concern level determines the consistency level required
of data being read.

The default level is C<undef>, which means the server will use its configured
default.

If the level is set to "local", reads will return the latest data a server has
locally.

Additional levels are storage engine specific.  See L<Read
Concern|http://docs.mongodb.org/manual/search/?query=readConcern> in the MongoDB
documentation for more details.

This may be set in a connection string with the the C<readConcernLevel> option.

=cut

has level => (
    is        => 'ro',
    isa       => Maybe [Str],
    predicate => 'has_level',
);

sub BUILD {
    my $self = shift;
    if ( defined $self->{level} ) {
        $self->{level} = lc $self->{level};
    }
}

# public interface for compatibility, but undocumented
sub as_args {
    my ( $self, $session ) = @_;

    # if session is defined and operation_time is not, then either the
    # operation_time was not sent on the response from the server for this
    # session or the session has causal consistency disabled.
    if ( $self->{level} ) {
        return [
            readConcern => {
              level => $self->{level},
              ( defined $session && defined $session->operation_time
                ? ( afterClusterTime => $session->operation_time )
                : () ),
            }
        ];
    }
    else {
        return [
            ( defined $session && defined $session->operation_time
              ? ( readConcern => { afterClusterTime => $session->operation_time } )
              : ()
            )
        ];
    }
}

1;

__END__

=head1 SYNOPSIS

    $rc = MongoDB::ReadConcern->new(); # no defaults

    $rc = MongoDB::ReadConcern->new(
        level    => 'local',
    );

=head1 DESCRIPTION

A Read Concern describes the constraints that MongoDB must satisfy when reading
data.  Read Concern was introduced in MongoDB 3.2.

=cut
