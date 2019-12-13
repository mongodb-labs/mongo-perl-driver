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
package MongoDB::_ServerSession;

# MongoDB Server Session object

use version;
our $VERSION = 'v2.2.2';

use MongoDB::Error;

use Moo;
use UUID::URandom;
use Math::BigInt;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    Maybe
    InstanceOf
    Int
    Bool
);
use constant UUID_TYPE => 4;

use namespace::clean -except => 'meta';

# session_id
#
#     $server_session->session_id;
#
# Returns the session id for this server session as a L<BSON::Bytes> object
# containing a binary UUID V4. For lower network usage, if not provided on
# initialisation this class will generate a new UUID instead of consulting the
# server for a new session id.

has session_id => (
    is => 'lazy',
    isa => Document,
    builder => '_build_session_id',
);

sub _build_session_id {
    my ( $self ) = @_;
    my $uuid = BSON::Bytes->new(
        data => UUID::URandom::create_uuid(),
        subtype => UUID_TYPE,
    );
    return { id => $uuid };
}

# last_use
#
#     $server_session->last_use;
#
# Returns the unix time that this server session was last used. Used for checking
# expiry of a server session. If undefined, then the session has (probably) not
# been used on the server.

has last_use => (
    is => 'rwp',
    init_arg => undef,
    isa => Maybe[Int],
);

# transaction_id
#
#     $server_session->transaction_id
#
# Returns the current transaction id for this server session. This is a ratcheted
# incrementing ID number, which when combined with the session id allows for
# retrying transactions in the correct order.

has transaction_id => (
    is => 'rwp',
    init_arg => undef,
    default => sub { Math::BigInt->new('0') },
);

# pool_epoch
#
# tracks which pool the session came from; sessions won't be checked into
# a newer pool

has pool_epoch => (
    is => 'ro',
    default => -1,
);

#
# Mark this session as dirty.
# A server session is marked dirty when a command fails with a network
# error. Dirty sessions are later discarded from the server session pool.
#

has dirty => (
    is => 'rwp',
    isa => Bool,
    default => 0,
);

sub mark_dirty {
    my $self = shift;
    $self->_set_dirty(1);
}

# update_last_use
#
#     $server_session->update_last_use;
#
# Updates the value of L</last_use> to the current unix time.

sub update_last_use {
    my ( $self ) = @_;
    $self->_set_last_use( time() );
}

sub _is_expiring {
    my ( $self, $session_timeout ) = @_;

    # if session_timeout is undef, then sessions arent actually supported (this
    # value should be from logical_session_timeout_minutes).
    return 1 unless defined $session_timeout;

    my $timeout = time() - ( ( $session_timeout - 1 ) * 60 );

    # Undefined last_use means its never actually been used on the server
    return 1 if defined $self->last_use && $self->last_use < $timeout;
    return;
}

1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
