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
package MongoDB::Role::_ClusterTimeModifier;

# MongoDB role to manage clusterTime commands

use Moo::Role;

use MongoDB::Error;
use MongoDB::_Types -types, 'to_IxHash';
use Scalar::Util qw/ blessed /;

use namespace::clean;

# As cluster time and sessions are hand in hand, makes sense to apply them here
with $_ for qw(
  MongoDB::Role::_SessionModifier
);

requires qw/client session/;

sub _apply_cluster_time {
    my ( $self, $link, $query_ref ) = @_;

    my $cluster_time;
    # TODO Check all consumers actually pass client
    if ( defined $self->client ) {
        $cluster_time = $self->client->_cluster_time;
        if ( defined $self->session
          && defined $self->session->cluster_time
          && ( $cluster_time->{'clusterTime'}->sec
             < $self->session->cluster_time->{'clusterTime'}->sec ) )
        {
            $cluster_time = $self->session->cluster_time;
        }
    } elsif ( defined $self->session ) {
        $cluster_time = $self->session->cluster_time;
    }

    # No cluster time in either session or client
    return unless defined $cluster_time;

    if ( $link->server->is_master->{maxWireVersion} >= 6 ) {
        # Gossip the clusterTime
        $$query_ref = to_IxHash( $$query_ref );
        ($$query_ref)->Push( '$clusterTime' => $cluster_time );
    }

    return;
}

sub _read_cluster_time {
    my ( $self, $response ) = @_;

    my $cluster_time;
    if ( blessed( $response ) && $response->isa( 'MongoDB::CommandResult' ) ) {
        $cluster_time = $response->output->{'$clusterTime'};
    } else {
        $cluster_time = $response->{'$clusterTime'};
    }

    return unless defined $cluster_time;

    $self->client->_update_cluster_time( $cluster_time ) if defined $self->client;

    $self->session->advance_cluster_time( $cluster_time ) if defined $self->session;

    return;
}

1;
