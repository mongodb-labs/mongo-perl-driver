#  Copyright 2018 - present MongoDB, Inc.
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
package MongoDB::ClientSession;

# ABSTRACT: MongoDB session management

use version;
our $VERSION = 'v1.999.1';

use MongoDB::Error;

use Moo;
use MongoDB::_Types qw(
    Document
    BSONTimestamp
);
use Types::Standard qw(
    Maybe
    HashRef
    InstanceOf
);
use namespace::clean -except => 'meta';

=attr client

The client this session was created using.  Sessions may only be used
with the client that created them.

=cut

has client => (
    is => 'ro',
    isa => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

=attr cluster_time

Stores the last received C<$clusterTime> for the client session. This is an
opaque value, to set it use the L<advance_cluster_time> function.

=cut

has cluster_time => (
    is => 'rwp',
    isa => Maybe[Document],
    init_arg => undef,
    default => undef,
);

=attr options

Options provided for this particular session. Available options include:

=for :list 
* C<causalConsistency> - If true, will enable causalConsistency for
  this session. For more information, see L<MongoDB documentation on Causal
  Consistency|https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/#causal-consistency>.
  Note that causalConsistency does not apply for unacknowledged writes.
  Defaults to true.


=cut

has options => (
    is => 'ro',
    isa => HashRef,
    required => 1,
    # Shallow copy to prevent action at a distance.
    # Upgrade to use Storable::dclone if a more complex option is required
    coerce => sub {
      $_[0] = {
        causalConsistency => 1,
        %{ $_[0] }
      };
    },
);

has _server_session => (
    is => 'ro',
    isa => InstanceOf['MongoDB::_ServerSession'],
    init_arg => 'server_session',
    required => 1,
    clearer => '__clear_server_session',
);

=attr operation_time

The last operation time. This is updated when an operation is performed during
this session, or when L</advance_operation_time> is called. Used for causal
consistency.

=cut

has operation_time => (
    is => 'rwp',
    isa => Maybe[BSONTimestamp],
    init_arg => undef,
    default => undef,
);

=method session_id

The session id for this particular session.  This should be considered
an opaque value.  If C<end_session> has been called, this returns C<undef>.

=cut

sub session_id {
    my ($self) = @_;
    return defined $self->_server_session ? $self->_server_session->session_id : undef;
}

=method get_latest_cluster_time

    my $cluster_time = $session->get_latest_cluster_time;

Returns the latest cluster time, when compared with this session's recorded
cluster time and the main client cluster time. If neither is defined, returns
undef.

=cut

sub get_latest_cluster_time {
    my ( $self ) = @_;

    # default to the client cluster time - may still be undef
    if ( ! defined $self->cluster_time ) {
        return $self->client->_cluster_time;
    }

    if ( defined $self->client->_cluster_time ) {
        # Both must be defined here so can just compare
        if ( $self->cluster_time->{'clusterTime'}
          > $self->client->_cluster_time->{'clusterTime'} ) {
            return $self->cluster_time;
        } else {
            return $self->client->_cluster_time;
        }
    }

    # Could happen that this cluster_time is updated manually before the client
    return $self->cluster_time;
}


=method advance_cluster_time

    $session->advance_cluster_time( $cluster_time );

Update the C<$clusterTime> for this session. Stores the value in
L</cluster_time>. If the cluster time provided is more recent than the sessions
current cluster time, then the session will be updated to this provided value.

Setting the C<$clusterTime> with a manually crafted value may cause a server
error. It is reccomended to only use C<$clusterTime> values retrieved from
database calls.

=cut

sub advance_cluster_time {
    my ( $self, $cluster_time ) = @_;

    # Only update the cluster time if it is more recent than the current entry
    if ( ! defined $self->cluster_time ) {
        $self->_set_cluster_time( $cluster_time );
    } else {
        if ( $cluster_time->{'clusterTime'}
          > $self->cluster_time->{'clusterTime'} ) {
            $self->_set_cluster_time( $cluster_time );
        }
    }
    return;
}

=method advance_operation_time

    $session->advance_operation_time( $operation_time );

Update the L</operation_time> for this session. If the value provided is more
recent than the sessions current operation time, then the session will be
updated to this provided value.

Setting C<operation_time> with a manually crafted value may cause a server
error. It is recommended to only use an C<operation_time> retreived from
another session or directly from a database call.

=cut

sub advance_operation_time {
    my ( $self, $operation_time ) = @_;

    # Just dont update operation_time if they've denied this, as it'l stop
    # everywhere else that updates based on this value from the session
    return unless $self->options->{causalConsistency};

    if ( !defined( $self->operation_time )
      || ( $operation_time > $self->operation_time ) ) {
        $self->_set_operation_time( $operation_time );
    }
    return;
}

=method end_session

    $session->end_session;

Close this particular session and release the session ID for reuse or
recycling.  Has no effect after calling for the first time.

=cut

sub end_session {
    my ( $self ) = @_;

    if ( defined $self->_server_session ) {
        $self->client->_server_session_pool->retire_server_session( $self->_server_session );
        $self->__clear_server_session;
    }
}

sub DEMOLISH {
    my ( $self, $in_global_destruction ) = @_;
    # Implicit end of session in scope
    $self->end_session;
}

1;

__END__

=pod

=head1 SYNOPSIS

    my $session = $client->start_session( $options );

    # use session in operations
    my $result = $collection->find( { id => 1 }, { session => $session } );

=head1 DESCRIPTION

This class encapsulates an active session for use with the current client.
Sessions support is new with MongoDB 3.6, and can be used in replica set and
sharded MongoDB clusters.

=head2 Explicit and Implicit Sessions

If you specifically apply a session to an operation, then the operation will be
performed with that session id. If you do not provide a session for an
operation, and the server supports sessions, then an implicit session will be
created and used for this operation.

The only exception to this is for unacknowledged writes - the driver will not
provide an implicit session for this, and if you provide a session then the
driver will raise an error.

=head2 Cursors

During cursors, if a session is not provided then an implicit session will be
created which is then used for the lifetime of the cursor. If you provide a
session, then note that ending the session and then continuing to use the
cursor will raise an error.

=head2 Thread Safety

Sessions are NOT thread safe, and should only be used by one thread at a time.
Using a session across multiple threads is unsupported and unexpected issues
and errors may occur. Note that the driver does not check for multi-threaded
use.

=cut
