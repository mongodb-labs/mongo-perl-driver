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

package MongoDB::Role::_LegacyReadPrefModifier;

# MongoDB interface for read ops that respect read preference

use version;
our $VERSION = 'v0.999.999.7';

use Moo::Role;

use MongoDB::Error;
use namespace::clean;

requires qw/read_preference/;

sub _apply_read_prefs {
    my ( $self, $link, $topology_type, $query_flags, $query_ref ) = @_;

    $topology_type ||= "<undef>";
    my $read_pref = $self->read_preference;

    if ( $topology_type eq 'Single' ) {
        if ( $link->server && $link->server->type eq 'Mongos' ) {
            $self->_apply_mongos_read_prefs($read_pref);
        }
        else {
            $query_flags->{slave_ok} = 1;
        }
    }
    elsif ( grep { $topology_type eq $_ } qw/ReplicaSetNoPrimary ReplicaSetWithPrimary/ ) {
        if ( !$read_pref || $read_pref->mode eq 'primary' ) {
            $query_flags->{slave_ok} = 0;
        }
        else {
            $query_flags->{slave_ok} = 1;
        }
    }
    elsif ( $topology_type eq 'Sharded' ) {
        $self->_apply_mongos_read_prefs($read_pref, $query_flags, $query_ref);
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
          unless $read_pref->has_empty_tag_sets;
    }
    else {
        MongoDB::InternalError->throw("invalid read preference mode '$mode'");
    }

    if ($need_read_pref) {
        if ( !$$query_ref->FETCH('$query') ) {
            $$query_ref = Tie::IxHash->new( '$query' => $$query_ref );
        }
        $$query_ref->Push( '$readPreference' => $read_pref->for_mongos );
    }

    return;
}

1;
