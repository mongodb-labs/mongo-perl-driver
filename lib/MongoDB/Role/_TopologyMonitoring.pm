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

package MongoDB::Role::_TopologyMonitoring;

# MongoDB role to add topology monitoring support

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;
use namespace::clean;

# These are used to cache data
has old_topology_desc => ( is => 'rw' );
has old_server_desc => ( is => 'rw' );

sub publish_topology_opening {
    my $self = shift;

    my $event = {
        topologyId => "$self",
        type => "topology_opening_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub publish_topology_closing {
    my $self = shift;

    my $event = {
        topologyId => "$self",
        type => "topology_closed_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub publish_server_opening {
    my ( $self, $address ) = @_;

    my $event = {
        topologyId => "$self",
        address => $address,
        type => "server_opening_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub publish_server_closing {
    my ( $self, $address ) = @_;

    my $event = {
        topologyId => "$self",
        address => $address,
        type => "server_closed_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub publish_server_heartbeat_started {
    my ($self, $link) = @_;

    my $event = {
        connectionId => $link->address,
        type => "server_heartbeat_started_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub publish_server_heartbeat_succeeded {
    my ($self, $link, $rtt_sec_fail, $is_master) = @_;

    my $event = {
        duration => $rtt_sec_fail,
        reply => $is_master,
        connectionId => $link->address,
        type => "server_heartbeat_succeeded_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub publish_server_heartbeat_failed {
    my ($self, $link, $rtt_sec_fail, $e) = @_;

    my $event = {
        duration => $rtt_sec_fail,
        failure => $e,
        connectionId => $link->address,
        type => "server_heartbeat_failed_event"
    };

    eval { $self->monitoring_callback->($event) };
}

sub __create_server_description {
    my ($self, $server) = @_;

    my $server_desc;

    if (defined $server->is_master) {
        $server_desc = {
            address => $server->address,
            error => $server->error,
            roundTripTime => $server->rtt_sec,
            lastWriteDate => $server->is_master->{lastWrite}->{lastWriteDate},
            opTime => $server->is_master->{opTime},
            type => $server->type || "Unknown",
            minWireVersion => $server->is_master->{min_wire_version},
            maxWireVersion => $server->is_master->{max_wire_version},
            me => $server->me,
            arbiters => $server->arbiters,
            hosts => $server->hosts,
            passives => $server->passives,
            (defined $server->is_master->{tags} ?
                (tags => $server->is_master->{tags}) : ()),
            ($server->primary ne "" ? (primary => $server->primary) : ()),
            (defined $server->is_master->{setName} ?
                (setName => $server->is_master->{setName}) : ()),
            (defined $server->is_master->{setVersion} ?
                (setVersion => $server->is_master->{setVersion}) : ()),
            (defined $server->is_master->{electionId} ?
                (electionId => $server->is_master->{electionId}) : ()),
            (defined $server->is_master->{logicalSessionTimeoutMinutes} ?
            (logicalSessionTimeoutMinutes =>
            $server->is_master->{logicalSessionTimeoutMinutes}) : ()),
        };
    } else {
        $server_desc = {
            address => $server->address,
            error => $server->error,
            roundTripTime => $server->rtt_sec,
            type => $server->type || "Unknown",
            me => $server->me,
            arbiters => $server->arbiters,
            hosts => $server->hosts,
            passives => $server->passives,
            #TODO figure out what tags should be
            tags => undef,
            ($server->primary ne "" ? (primary => $server->primary) : ()),
        };
    }

    return $server_desc;
}

sub __has_changed_servers {
  my ($self, $new_server ) = @_;

  # Fields considered Server Description equality
  my $equal_servers = 1;
  my %equality_fields = (
      address => 1,
      type => 1,
      minWireVersion => 1,
      minWireVersion => 1,
      me => 1,
      arbiters => 1,
      hosts => 1,
      passives => 1,
      tags => 1,
      primary => 1,
      setName => 1,
      setVersion => 1,
      electionId => 1,
      logicalSessionTimeoutMinutes => 1,
  );
  my $new_server_desc = $self->__create_server_description($new_server);

  my %oldhash = %{$self->old_server_desc};
  my %newhash = %{$new_server_desc};

  foreach my $key (keys %newhash) {
      if (exists($equality_fields{$key})) {
          if (!exists($oldhash{$key})) {
              $equal_servers = 0;
              last;
          } elsif (!defined($newhash{$key}) &&
                      !defined($oldhash{$key})) {
              next;
          } elsif ($newhash{$key} ne $oldhash{$key}) {
              $equal_servers = 0;
              last;
          }
      }
  }

  unless ( $equal_servers ) {
      my $event_server = {
          topologyId => "$self",
          address => $new_server->address,
          previousDescription => $self->old_server_desc,
          newDescription => $new_server_desc,
          type => "server_description_changed_event"
      };

      eval { $self->monitoring_callback->($event_server) };
  }
}

sub publish_old_topology_desc {
    my ( $self, $address, $new_server ) = @_;

    if ( $address ) {
        my $server = $self->servers->{$address};
        my $old_server = $self->__create_server_description($server);
        $self->old_server_desc($old_server);
    }

    if ( $new_server ) {
        $self->__has_changed_servers($new_server);
    }

    $self->old_topology_desc( $self->__create_topology_description );
}

sub publish_new_topology_desc {
    my $self = shift;

    my $event_topology = {
        topologyId => "$self",
        previousDescription => $self->old_topology_desc,
        newDescription => $self->__create_topology_description,
        type => "topology_description_changed_event"
    };

    eval { $self->monitoring_callback->($event_topology) };
}

sub __create_topology_description {
    my ( $self ) = @_;

    my @servers = map { $self->__create_server_description($_) } $self->all_servers;

    return {
        topologyType => $self->type,
        ( $self->replica_set_name ne ""
          ? ( setName => $self->replica_set_name )
          : ()
        ),
        ( defined $self->max_set_version
          ? ( maxSetVersion => $self->max_set_version )
          : ()
        ),
        ( defined $self->max_election_id
          ? ( maxElectionId => $self->max_election_id )
          : ()
        ),
        servers => \@servers,
        stale => $self->stale,
        ( defined $self->is_compatible
          ? ( compatible => $self->is_compatible )
          : ()
        ),
        ( defined $self->compatibility_error
          ? ( compatibilityError => $self->compatibility_error )
          : ()
        ),
        ( defined $self->logical_session_timeout_minutes
          ? ( logicalSessionTimeoutMinutes => $self->logical_session_timeout_minutes )
          : ()
        ),
    };
}

1;
