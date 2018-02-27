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
package MongoDB::ServerSession;

# ABSTRACT: MongoDB Server Session object

use MongoDB::Error;

use Moo;
use DateTime;
use UUID::Tiny ':std'; # Use newer interface
use MongoDB::BSON::Binary;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    Maybe
    InstanceOf
);
use namespace::clean -except => 'meta';

has session_id => (
    is => 'lazy',
    isa => Document,
    builder => '_build_session_id',
);

sub _build_session_id {
    my ( $self ) = @_;
    my $uuid = MongoDB::BSON::Binary->new(
        data => create_uuid(UUID_V4),
        subtype => MongoDB::BSON::Binary->SUBTYPE_UUID,
    );
    return { id => $uuid };
}

has last_use => (
    is => 'rwp',
    init_arg => undef,
    isa => Maybe[InstanceOf['DateTime']],
);

sub update_last_use {
    my ( $self ) = @_;
    $self->_set_last_use( DateTime->now );
}

sub _is_expiring {
    my ( $self, $session_timeout ) = @_;

    my $timeout = DateTime->now;
    $timeout->subtract( minutes => $session_timeout - 1 );

    # Undefined last_use means its never actually been used on the server
    return 1 if defined $self->last_use && $self->last_use < $timeout;
    return;
}

1;
