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
use Devel::Dwarn;

use namespace::clean;

requires qw/client/;

sub _apply_cluster_time {
    my ( $self, $link, $query_ref ) = @_;

    # TODO Check all consumers actually pass client
    return unless defined $self->client;
    return unless defined $self->client->cluster_time;

    if ( $link->server->is_master->{maxWireVersion} >= 6 ) {
        $$query_ref = to_IxHash( $$query_ref );
        ($$query_ref)->Push( '$clusterTime' => $self->client->cluster_time );
    }

    return;
}

sub _read_cluster_time {
    my ( $self, $response ) = @_;

    return unless defined $self->client;

    my $cluster_time;
    if ( blessed( $response ) && $response->isa( 'MongoDB::CommandResult' ) ) {
        $cluster_time = $response->output->{'$clusterTime'};
    } else {
        $cluster_time = $response->{'$clusterTime'};
    }

    return unless defined $cluster_time;

    $self->client->_update_cluster_time( $cluster_time );
    return;
}

1;
