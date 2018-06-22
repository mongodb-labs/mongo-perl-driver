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
package MongoDB::Role::_SessionSupport;

# MongoDB role to add support for sessions on Ops

use version;
our $VERSION = 'v1.999.1';

use Moo::Role;
use MongoDB::_Types -types, 'to_IxHash';
use Safe::Isa;
use boolean;
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

    if ( $self->retryable_write || ! $self->session->_in_transaction_state( 'none' ) ) {
        ($$query_ref)->Push( 'txnNumber' => $self->session->_server_session->transaction_id );
    }

    if ( ! $self->session->_in_transaction_state( qw/ none / ) ) {
        ($$query_ref)->Push( 'autocommit' => false );
    }

    if ( $self->session->_in_transaction_state( 'starting' ) ) {
        ($$query_ref)->Push( 'startTransaction' => true );
        ($$query_ref)->Push( @{ $self->session->_get_transaction_read_concern->as_args( $self->session ) } );
    }

    # write concern not allowed in transactions except when ending. We can
    # safely delete it here as you can only pass writeConcern through by
    # arguments to client of collection.
    if ( $self->session->_in_transaction_state( qw/ starting in_progress / ) ) {
        ($$query_ref)->Delete( 'writeConcern' );
    }

    # read concern only valid outside a transaction or when starting
    if ( ! $self->session->_in_transaction_state( qw/ none starting / ) ) {
        ($$query_ref)->Delete( 'readConcern' );
    }

    if ( $self->session->_in_transaction_state( qw/ aborted committed / )
         && ! ($$query_ref)->EXISTS('writeConcern')
    ) {
        ($$query_ref)->Push( @{ $self->session->_transaction_write_concern->as_args() } );
    }

    $self->session->_server_session->update_last_use;

    my $cluster_time = $self->session->get_latest_cluster_time;

    if ( defined $cluster_time && $link->supports_clusterTime ) {
        # Gossip the clusterTime
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

sub _update_session_pre_assert {
    my ( $self, $response ) = @_;

    return unless defined $self->session;

    if ( $self->session->_in_transaction_state( 'starting' ) ) {
        $self->session->_set__transaction_state( 'in_progress' );
    }

    my $operation_time = $self->__extract_from( $response, 'operationTime' );
    $self->session->advance_operation_time( $operation_time ) if defined $operation_time;

    return;
}

# Certain errors have to happen as soon as possible, such as write concern
# errors in a retryable write. This has to be seperate to the other functions
# due to not all result objects having the base response inside, so cannot be
# used to parse operationTime or $clusterTime
sub _assert_session_errors {
    my ( $self, $response ) = @_;

    if ( $self->retryable_write ) {
        $response->assert_no_write_concern_error;
    }

    return;
}

sub _update_session_connection_error {
    my ( $self, $err ) = @_;

    if ( $self->session->_in_transaction_state( qw/ starting in_progress / ) ) {
        push @{ $err->error_labels }, 'TransientTransactionError';
        # If already in_progress, no harm done
        $self->session->_set__transaction_state( 'in_progress' );
    }
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
