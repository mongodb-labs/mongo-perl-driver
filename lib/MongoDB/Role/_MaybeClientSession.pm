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

use strict;
use warnings;
package MongoDB::Role::_MaybeClientSession;

# MongoDB role to hold Op sessions and create implicit ones

use Moo::Role;

use namespace::clean;

requires qw/client/;

has session => (
    is => 'rwp',
    lazy => 1,
    builder => '_build_session',
);

# Should only be called when making an implicit session
sub _build_session {
    my ( $self ) = @_;

    # Cant create a session without a client
    return unless defined $self->client;

    return $self->client->_start_implicit_session;
}

1;