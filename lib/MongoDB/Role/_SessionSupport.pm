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
package MongoDB::Role::_SessionSupport;

# MongoDB role to add support for sessions on Ops

use version;
our $VERSION = 'v1.999.0';

use Moo::Role;
use MongoDB::_Types -types, 'to_IxHash';
use Safe::Isa;
use namespace::clean;

requires qw/ session retryable_write /;

sub _apply_session_and_cluster_time {
    my ( $self, $link, $query_ref ) = @_;

    # Assume that no session means no session support. Also means no way of
    # getting clusterTime.
    return unless defined $self->session;

    # Also assumption that the session was created from the current client -
    # not an issue in implicit sessions, but explicit sessions may have been
    # created by another client in the same scope. This should have been
    # checked further up the call chain

    $$query_ref = to_IxHash( $$query_ref );
    ($$query_ref)->Push( 'lsid' => $self->session->session_id );

    if ( $self->retryable_write ) {
        ($$query_ref)->Push( 'txnNumber' => $self->session->_server_session->transaction_id );
    }

    $self->session->_server_session->update_last_use;

    my $cluster_time = $self->session->get_latest_cluster_time;

    if ( defined $cluster_time && $link->supports_clusterTime ) {
        # Gossip the clusterTime
        $$query_ref = to_IxHash($$query_ref);
        ($$query_ref)->Push( '$clusterTime' => $cluster_time );
    }

    return;
}

# Somethign about this makes me wonder if clustertime is not going to be set
# when it should: not all requests have sessions, but any response has the
# chance to have a clustertime. If we do not have a client available to put the
# clustertime in, then depending on the response there may be no way to
# retreive the clustertime further down the call stack.
sub _update_session_and_cluster_time {
    my ( $self, $response ) = @_;

    # No point continuing as theres nothing to do even if clusterTime is returned
    return unless defined $self->session;

    my $cluster_time = $self->__extract_from( $response, '$clusterTime' );

    if ( defined $cluster_time ) {
        $self->session->client->_update_cluster_time( $cluster_time );
        $self->session->advance_cluster_time( $cluster_time );
    }

    return;
}

sub _update_operation_time {
    my ( $self, $response ) = @_;

    return unless defined $self->session;

    my $operation_time = $self->__extract_from( $response, 'operationTime' );
    $self->session->advance_operation_time( $operation_time ) if defined $operation_time;

    return;
}

sub __extract_from {
    my ( $self, $response, $key ) = @_;

    if ( $response->$_isa( 'MongoDB::CommandResult' ) ) {
        return $response->output->{ $key };
    } else {
        return $response->{ $key };
    }
}

1;
