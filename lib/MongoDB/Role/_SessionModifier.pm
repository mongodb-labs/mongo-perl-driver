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
package MongoDB::Role::_SessionModifier;

# MongoDB role to manage clusterTime commands

use Moo::Role;
use MongoDB::Error;
use MongoDB::_Types -types, 'to_IxHash';

use namespace::clean;

requires qw/client/;

with $_ for qw(
  MongoDB::Role::_MaybeClientSession
);

sub _apply_session {
    my ( $self, $query_ref ) = @_;

    return unless defined $self->session;

    if ( defined $self->client
      && ( $self->client->_id ne $self->session->client->_id ) )
    {
        # Cannot use a session from another client! bad things happen!
        # Note that this will not happen for implicit sessions as they are
        # defined above
        # TODO Is there a specific error message?
        MongoDB::Error->throw( "Cannot use session from another client" );
    }    

    $$query_ref = to_IxHash( $$query_ref );
    ($$query_ref)->Push( 'lsid' => $self->session->server_session->session_id );
    
    $self->session->server_session->update_last_use;

    return;
}

sub _retire_implicit_session {
    my ( $self ) = @_;

    return unless defined $self->session;

    $self->session->end_session unless $self->session->is_explicit;
    return;
}

1;
