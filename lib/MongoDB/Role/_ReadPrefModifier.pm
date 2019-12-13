#  Copyright 2014 - present MongoDB, Inc.
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

package MongoDB::Role::_ReadPrefModifier;

# MongoDB role to modify OP_QUERY query document or flags to account
# for topology-specific treatment of a read-preference (if any)

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::Error;
use MongoDB::ReadPreference;
use MongoDB::_Types -types, 'to_IxHash';

use namespace::clean;

requires qw/read_preference/;

my $PRIMARY = MongoDB::ReadPreference->new()->_as_hashref;
my $PRIMARYPREFERRED =
  MongoDB::ReadPreference->new( mode => 'primaryPreferred' )->_as_hashref;

sub _apply_op_msg_read_prefs {
    my ( $self, $link, $topology_type, $query_flags, $query_ref ) = @_;

    $topology_type ||= "<undef>";
    my $read_pref = $self->read_preference;
    my $read_pref_doc = $read_pref ? $read_pref->_as_hashref : $PRIMARY;

    if ( $topology_type eq 'Single' && ! ($link->server && $link->server->type eq 'Mongos') ) {
        # For direct connection to a non-mongos single server, allow any server
        # type, overriding the provided read preference
        $read_pref_doc = $PRIMARYPREFERRED;
    }

    $$query_ref = to_IxHash($$query_ref);
    ($$query_ref)->Push( '$readPreference' => $read_pref_doc );

    return;
}

sub _apply_op_query_read_prefs {
    my ( $self, $link, $topology_type, $query_flags, $query_ref ) = @_;

    $topology_type ||= "<undef>";
    my $read_pref = $self->read_preference;

    if ( $topology_type eq 'Single' ) {
        if ( $link->server && $link->server->type eq 'Mongos' ) {
            $self->_apply_mongos_read_prefs( $read_pref, $query_flags, $query_ref );
        }
        else {
            $query_flags->{slave_ok} = 1;
        }
    }
    elsif ( grep { $topology_type eq $_ } qw/ReplicaSetNoPrimary ReplicaSetWithPrimary/ )
    {
        if ( !$read_pref || $read_pref->mode eq 'primary' ) {
            $query_flags->{slave_ok} = 0;
        }
        else {
            $query_flags->{slave_ok} = 1;
        }
    }
    elsif ( $topology_type eq 'Sharded' ) {
        $self->_apply_mongos_read_prefs( $read_pref, $query_flags, $query_ref );
    }
    else {
        MongoDB::InternalError->throw("can't query topology type '$topology_type'");
    }

    return;
}

sub _apply_mongos_read_prefs {
    my ( $self, $read_pref, $query_flags, $query_ref ) = @_;
    my $mode = $read_pref ? $read_pref->mode : 'primary';
    my $need_read_pref;

    if ( $mode eq 'primary' ) {
        $query_flags->{slave_ok} = 0;
    }
    elsif ( grep { $mode eq $_ } qw/secondary primaryPreferred nearest/ ) {
        $query_flags->{slave_ok} = 1;
        $need_read_pref = 1;
    }
    elsif ( $mode eq 'secondaryPreferred' ) {
        $query_flags->{slave_ok} = 1;
        $need_read_pref = 1
          unless $read_pref->has_empty_tag_sets && $read_pref->max_staleness_seconds == -1;
    }
    else {
        MongoDB::InternalError->throw("invalid read preference mode '$mode'");
    }

    if ($need_read_pref) {
        $$query_ref = to_IxHash($$query_ref);
        if ( !($$query_ref)->FETCH('$query') ) {
            $$query_ref = Tie::IxHash->new( '$query' => $$query_ref );
        }
        ($$query_ref)->Push( '$readPreference' => $read_pref->_as_hashref );
    }

    return;
}

1;
