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

# TODO Documentation

use MongoDB::Error;

use Moo;
use MongoDB::ServerSession;
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
to the pool of this client when this Client Session is closed.

=cut

has client => (
    is => 'ro',
    isa => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

=method cluster_time

Stores the last received C<$clusterTime> for the client session.

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
    isa => Maybe[InstanceOf['MongoDB::ServerSession']],
    required => 1,
);

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

=method advance_cluser_time

    $session->advance_cluster_time( $cluster_time );

Update the C<$clusterTime> for this session. If the cluster time provided is
more recent than the sessions current cluster time, then the session will be
updated to this provided value.

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
