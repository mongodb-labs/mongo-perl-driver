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
package MongoDB::ClientSession;

# ABSTRACT: MongoDB session management

use MongoDB::Error;

use Moo;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    Bool
    Maybe
    HashRef
    InstanceOf
);
use namespace::clean -except => 'meta';

=method client

The client this session was created using. The server session will be returned
to the pool of this client when this client session is closed.

=cut

has client => (
    is => 'ro',
    isa => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

=method cluster_time

Stores the last received C<$clusterTime> for the client session. This is an
opaque value, to set it use the L<advance_cluster_time> function.

=cut

has cluster_time => (
    is => 'rwp',
    isa => Maybe[Document],
    init_arg => undef,
    default => undef,
);

=method options

Options provided for this particular session.

=cut

has options => (
    is => 'ro',
    isa => HashRef,
    required => 1,
    # Shallow copy to prevent action at a distance.
    # Upgrade to use Storable::dclone if a more complex option is required
    coerce => sub {
      $_[0] = { %{ $_[0] } };
    },
);

=method server_session

The server session containing the unique id for this session. See
L<MongoDB::ServerSession> for more information.

=cut

has server_session => (
    is => 'rwp',
    isa => Maybe[InstanceOf['MongoDB::_ServerSession']],
    required => 1,
);

#--------------------------------------------------------------------------#
# private attributes for internal use
#--------------------------------------------------------------------------#

has _is_explicit => (
    is => 'ro',
    isa => Bool,
    default => 0,
);

has _in_cursor => (
    is => 'rw',
    isa => Bool,
    default => 0,
);

has _has_ended => (
    is => 'rwp',
    isa => Bool,
    default => 0,
);

# Check if this should be ended as an implicit session. Returns truthy if this
# session should be ended as an implicit session.
sub _should_end_implicit {
    my ( $self ) = @_;

    return if $self->_in_cursor;
    return if $self->_is_explicit;
    return 1;
}

=method session_id

The session id for this particular session. See
L<MongoDB::ServerSession/session_id> for more information.

=cut

sub session_id {
    my ( $self ) = @_;
    return $self->server_session->session_id;
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
error. It is reccomended to only use C<$clusterTime> values retreived from
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

=method end_session

    $session->end_session;

Close this particular session and return the Server Session back to the
client's session pool. Has no effect after calling for the first time.

=cut

sub end_session {
    my ( $self ) = @_;

    if ( defined $self->server_session ) {
        $self->client->_server_session_pool->retire_server_session( $self->server_session );
        $self->_set_server_session( undef );
        $self->_set__has_ended( 1 );
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
