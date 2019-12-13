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
package MongoDB::_SessionPool;

use version;
our $VERSION = 'v2.2.2';

use Moo;
use MongoDB::_ServerSession;
use Types::Standard qw(
    ArrayRef
    InstanceOf
);

has dispatcher => (
    is => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::_Dispatcher'],
);

has topology=> (
    is => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::_Topology'],
);

has _server_session_pool => (
    is => 'lazy',
    isa => ArrayRef[InstanceOf['MongoDB::_ServerSession']],
    init_arg => undef,
    clearer => 1,
    builder => sub { [] },
);

has _pool_epoch => (
    is => 'rwp',
    init_arg => undef,
    default => 0,
);

# Returns a L<MongoDB::ServerSession> that was at least one minute remaining
# before session times out. Returns undef if no sessions available.
#
# Also retires any expiring sessions from the front of the queue as requried.

sub get_server_session {
    my ( $self ) = @_;

    if ( scalar( @{ $self->_server_session_pool } ) > 0 ) {
        my $session_timeout = $self->topology->logical_session_timeout_minutes;
        # if undefined, sessions not actually supported so drop out here
        while ( my $session = shift @{ $self->_server_session_pool } ) {
            next if $session->_is_expiring( $session_timeout );
            return $session;
        }
    }
    return MongoDB::_ServerSession->new( pool_epoch => $self->_pool_epoch );
}

# Place a session back into the pool for use. Will check that there is at least
# one minute remaining in the session, and if so will place the session at the
# front of the pool.
#
# Also checks for expiring sessions at the back of the pool, and retires as
# required.

sub retire_server_session {
    my ( $self, $server_session ) = @_;

    return if $server_session->pool_epoch != $self->_pool_epoch;

    my $session_timeout = $self->topology->logical_session_timeout_minutes;

    # Expire old sessions from back of queue
    while ( my $session = $self->_server_session_pool->[-1] ) {
        last unless $session->_is_expiring( $session_timeout );
        pop @{ $self->_server_session_pool };
    }

    unless ( $server_session->_is_expiring( $session_timeout ) ) {
        unshift @{ $self->_server_session_pool }, $server_session
            unless $server_session->dirty;
    }
    return;
}

# Close all sessions registered with the server. Used during global cleanup.

sub end_all_sessions {
    my ( $self ) = @_;

    my @batches;
    push @batches,
        [ splice @{ $self->_server_session_pool }, 0, 10_000 ]
            while @{ $self->_server_session_pool };

    for my $batch ( @batches ) {
        my $sessions = [
            map { defined $_ ? $_->session_id : () } @$batch
        ];
        # Ignore any errors generated from this
        eval {
            $self->dispatcher->send_admin_command([
                endSessions => $sessions,
            ], 'primaryPreferred');
        };
    }
}

# When reconnecting a client after a fork, we need to clear the pool
# without ending sessions with the server and increment the pool epoch
# so existing sessions aren't checked back in.
sub reset_pool {
    my ( $self ) = @_;
    $self->_clear_server_session_pool;
    $self->_set__pool_epoch( $self->_pool_epoch + 1 );
}

sub DEMOLISH {
    my ( $self, $in_global_destruction ) = @_;

    $self->end_all_sessions;
}

1;
