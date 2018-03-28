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
package MongoDB::_SessionPool;

use Moo;
use MongoDB::_ServerSession;
use Types::Standard qw(
    ArrayRef
    InstanceOf
);

has client => (
    is => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

has _server_session_pool => (
    is => 'lazy',
    isa => ArrayRef[InstanceOf['MongoDB::_ServerSession']],
    init_arg => undef,
    builder => sub { [] },
);

=method get_server_session

    my $session = $pool->get_server_session;

Returns a L<MongoDB::ServerSession> that was at least one minute remaining
before session times out. Returns undef if no sessions available.

Also retires any expiring sessions from the front of the queue as requried.

=cut

sub get_server_session {
    my ( $self ) = @_;

    if ( scalar( @{ $self->_server_session_pool } ) > 0 ) {
        my $session_timeout = $self->client->_topology->logical_session_timeout_minutes;
        # if undefined, sessions not actually supported so drop out here
        while ( my $session = shift @{ $self->_server_session_pool } ) {
            next if $session->_is_expiring( $session_timeout );
            return $session;
        }
    }
    return MongoDB::_ServerSession->new;
}

=method retire_server_session

    $pool->retire_server_session( $session );

Place a session back into the pool for use. Will check that there is at least
one minute remaining in the session, and if so will place the session at the
front of the pool.

Also checks for expiring sessions at the back of the pool, and retires as
required.

=cut

sub retire_server_session {
    my ( $self, $server_session ) = @_;

    my $session_timeout = $self->client->_topology->logical_session_timeout_minutes;

    # Expire old sessions from back of queue
    while ( my $session = $self->_server_session_pool->[-1] ) {
        last unless $session->_is_expiring( $session_timeout );
        pop @{ $self->_server_session_pool };
    }

    unless ( $server_session->_is_expiring( $session_timeout ) ) {
        unshift @{ $self->_server_session_pool }, $server_session;
    }
    return;
}

=method end_all_sessions

    $pool->end_all_sessions

Close all sessions registered with the server. Used during global cleanup.

=cut

sub end_all_sessions {
    my ( $self ) = @_;

    my @batches;
    push @batches,
        [ splice @{ $self->_server_session_pool }, 0, 10_000 ]
            while @{ $self->_server_session_pool };

    for my $batch ( @batches ) {
        my $sessions = [
            # TODO For some reason, the sessions are getting demolished before
            # here, even though we have references to them...
            map { defined $_ ? $_->session_id : () } @$batch
        ];
        # Ignore any errors generated from this
        eval {
            $self->client->send_admin_command([
                endSessions => $sessions,
            ], 'primaryPreferred');
        };
    }
}

sub DEMOLISH {
    my ( $self, $in_global_destruction ) = @_;

    $self->end_all_sessions;
}

1;
