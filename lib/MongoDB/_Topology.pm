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
package MongoDB::_Topology;

use version;
our $VERSION = 'v2.2.2';

use Moo;
use BSON;
use MongoDB::Error;
use MongoDB::Op::_Command;
use MongoDB::_Platform;
use MongoDB::ReadPreference;
use MongoDB::_Constants;
use MongoDB::_Link;
use MongoDB::_Types qw(
    Boolish
    BSONCodec
    CompressionType
    Document
    NonNegNum
    TopologyType
    ZlibCompressionLevel
    to_IxHash
);
use Types::Standard qw(
    CodeRef
    HashRef
    ArrayRef
    InstanceOf
    Num
    Str
    Maybe
);
use MongoDB::_Server;
use MongoDB::_Protocol;
use Config;
use List::Util qw/first max min/;
use Safe::Isa;
use Time::HiRes qw/time usleep/;

use namespace::clean;

with $_ for qw(
  MongoDB::Role::_TopologyMonitoring
);

#--------------------------------------------------------------------------#
# attributes
#--------------------------------------------------------------------------#

has uri => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::_URI'],
);

has min_server_version => (
    is       => 'ro',
    required => 1,
    isa => Str,
);

has max_wire_version => (
    is       => 'ro',
    required => 1,
    isa => Num,
);

has min_wire_version => (
    is       => 'ro',
    required => 1,
    isa => Num,
);

has credential => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::_Credential'],
);

# Required so it's passed explicitly, even if undef, to ensure it's wired
# up correctly.
has monitoring_callback => (
    is => 'ro',
    required => 1,
    isa => Maybe[CodeRef],
);

has compressors => (
    is => 'ro',
    isa => ArrayRef[CompressionType],
    default => sub { [] },
);

has zlib_compression_level => (
    is => 'ro',
    isa => ZlibCompressionLevel,
    default => sub { -1 },
);

has type => (
    is      => 'ro',
    writer  => '_set_type',
    default => 'Unknown',
    isa => TopologyType,
);

has app_name => (
    is      => 'ro',
    default => '',
    isa     => Str,
);

has replica_set_name => (
    is      => 'ro',
    default => '',
    writer  => '_set_replica_set_name', # :-)
    isa => Str,
);

has heartbeat_frequency_sec => (
    is      => 'ro',
    default => 60,
    isa => NonNegNum,
);

has last_scan_time => (
    is      => 'ro',
    default => EPOCH,
    writer  => '_set_last_scan_time',
    isa => Num,
);

has local_threshold_sec => (
    is      => 'ro',
    default => 0.015,
    isa => Num,
);

has logical_session_timeout_minutes => (
    is => 'ro',
    default => undef,
    writer => '_set_logical_session_timeout_minutes',
    isa => Maybe [NonNegNum],
);

has next_scan_time => (
    is      => 'ro',
    default => sub { time() },
    writer  => '_set_next_scan_time',
    isa => Num,
);

has socket_check_interval_sec => (
    is      => 'ro',
    default => 5,
    isa => Num,
);

has server_selection_timeout_sec => (
    is      => 'ro',
    default => 60,
    isa => Num,
);

has server_selection_try_once => (
    is      => 'ro',
    default => 1,
    isa => Boolish,
);

has server_selector => (
    is => 'ro',
    isa => Maybe[CodeRef],
);

has ewma_alpha => (
    is      => 'ro',
    default => 0.2,
    isa => Num,
);

has link_options => (
    is      => 'ro',
    default => sub { {} },
    isa => HashRef,
);

has bson_codec => (
    is       => 'ro',
    default  => sub { BSON->new },
    isa => BSONCodec,
    init_arg => undef,
);

has number_of_seeds => (
    is      => 'lazy',
    builder => '_build_number_of_seeds',
    isa => Num,
);

has max_election_id => (
    is      => 'rw',
    writer  => '_set_max_election_id',
);

has max_set_version => (
    is     => 'rw',
    isa    => Maybe [Num],
    writer => '_set_max_set_version',
);

# generated only once per _Topology, for performance
has handshake_document => (
    is      => 'lazy',
    isa     => Document,
    builder => '_build_handshake_document',
);

# compatible wire protocol
has is_compatible => (
    is => 'ro',
    writer => '_set_is_compatible',
    isa => Boolish,
);

has compatibility_error => (
    is      => 'ro',
    default => '',
    writer => '_set_compatibility_error',
    isa     => Str,
);

has wire_version_floor => (
    is => 'ro',
    writer => '_set_wire_version_floor',
    default => 0,
);

has wire_version_ceil => (
    is => 'ro',
    writer => '_set_wire_version_ceil',
    default => 0,
);

has current_primary => (
    is => 'rwp',
    clearer => '_clear_current_primary',
    init_arg => undef,
);

has stale => (
    is => 'rwp',
    init_arg => undef,
    default => 1,
);

# servers, links and rtt_ewma_sec are all hashes on server address

has servers => (
    is      => 'ro',
    default => sub { {} },
    isa => HashRef[InstanceOf['MongoDB::_Server']],
);

has _incompatible_servers => (
    is => 'rw',
    default => sub { [] },
    isa => ArrayRef[InstanceOf['MongoDB::_Server']],
);

has links => (
    is      => 'ro',
    default => sub { {} },
    isa => HashRef[InstanceOf['MongoDB::_Link']],
);

has rtt_ewma_sec => (
    is      => 'ro',
    default => sub { {} },
    isa => HashRef[Num],
);

has cluster_time => (
    is => 'rwp',
    isa => Maybe[Document],
    init_arg => undef,
    default => undef,
);

sub update_cluster_time {
    my ( $self, $cluster_time ) = @_;

    return unless $cluster_time && exists $cluster_time->{clusterTime}
        && ref($cluster_time->{clusterTime}) eq 'BSON::Timestamp';

    # Only update the cluster time if it is more recent than the current entry
    if ( !defined $self->cluster_time ) {
        $self->_set_cluster_time($cluster_time);
    }
    elsif ( $cluster_time->{'clusterTime'} > $self->cluster_time->{'clusterTime'} ) {
        $self->_set_cluster_time($cluster_time);
    }
    return;
}

#--------------------------------------------------------------------------#
# builders
#--------------------------------------------------------------------------#

sub _build_number_of_seeds {
    my ($self) = @_;
    return scalar @{ $self->uri->hostids };
}

sub _truncate_for_handshake {
    my $str = shift;
    return substr( $str, 0, 64 );
}

sub _build_handshake_document {
    my ($self) = @_;
    my $driver_version_without_leading_v = substr( $VERSION, 1 );

    return to_IxHash(
        [
            ( length( $self->app_name ) ? ( application => { name => $self->app_name } ) : () ),
            driver => to_IxHash(
                [
                    name    => "MongoDB Perl Driver",
                    version => $driver_version_without_leading_v,
                ]
            ),
            os => { type => _truncate_for_handshake(MongoDB::_Platform::os_type) },
            platform => _truncate_for_handshake(MongoDB::_Platform::platform_details)
        ]
    );
}

sub BUILD {
    my ($self) = @_;

    $self->publish_topology_opening
      if $self->monitoring_callback;

    $self->publish_old_topology_desc
      if $self->monitoring_callback;
    my $type = $self->type;
    my @addresses = @{ $self->uri->hostids };

    # clone bson codec to disable dt_type
    $self->{bson_codec} = $self->bson_codec->clone( dt_type => undef );

    if ( my $set_name = $self->replica_set_name ) {
        if ( $type eq 'Single' || $type eq 'ReplicaSetNoPrimary' ) {
            # these are valid, so nothing to do here
        }
        elsif ( $type eq 'Unknown' ) {
            $self->_set_type('ReplicaSetNoPrimary');
        }
        else {
            MongoDB::InternalError->throw(
                "deployment with set name '$set_name' may not be initialized as type '$type'");
        }
    }

    if ( $type eq 'Single' && @addresses > 1 ) {
        MongoDB::InternalError->throw(
            "topology type 'Single' cannot be used with multiple addresses: @addresses");
    }

    $self->_add_address_as_unknown($_) for @addresses;

    $self->publish_new_topology_desc
      if $self->monitoring_callback;

    return;
}

sub DEMOLISH {
    my $self = shift;

    $self->publish_topology_closing
      if $self->monitoring_callback;

    return;
}

sub _check_for_uri_changes {
    my ($self) = @_;

    my $type = $self->type;
    return unless
        $type eq 'Sharded'
        or $type eq 'Unknown';

    my @existing = @{ $self->uri->hostids };

    my $options = {
        fallback_ttl_sec => $self->{heartbeat_frequency_sec},
    };

    if ($self->uri->check_for_changes($options)) {
        my %new = map { ($_, 1) } @{ $self->uri->hostids };
        for my $address (@existing) {
            if (!$new{$address}) {
                $self->_remove_address($address);
            }
            else {
                delete $new{$address};
            }
        }
        for my $address (keys %new) {
            $self->_add_address_as_unknown($address);
        }
    }
}

#--------------------------------------------------------------------------#
# public methods
#--------------------------------------------------------------------------#

sub all_servers { return values %{ $_[0]->servers } }

sub all_data_bearing_servers { return grep { $_->is_data_bearing } $_[0]->all_servers }

sub check_address {
    my ( $self, $address ) = @_;

    my $link = $self->links->{$address};
    if ( $link && $link->is_connected ) {
        $self->_update_topology_from_link( $link, with_handshake => 0 );
    }
    else {
        # initialize_link will call update_topology_from_link
        $self->_initialize_link($address);
    }

    return;
}

sub close_all_links {
    my ($self) = @_;
    delete $self->links->{ $_->address } for $self->all_servers;
    return;
}

sub _maybe_get_txn_error_labels_and_unpin_from {
    my $op = shift;
    return () unless defined $op
        && defined $op->session;
    if ( $op->session->_in_transaction_state( TXN_STARTING, TXN_IN_PROGRESS ) ) {
        $op->session->_unpin_address;
        return ( error_labels => [ TXN_TRANSIENT_ERROR_MSG ] );
    } elsif ( $op->session->_in_transaction_state( TXN_COMMITTED ) ) {
        return ( error_labels => [ TXN_UNKNOWN_COMMIT_MSG ] );
    }
    return ();
}

sub get_readable_link {
    my ( $self, $op ) = @_;
    $self->_check_for_uri_changes;

    my $read_pref = defined $op ? $op->read_preference : undef;

    my $mode = $read_pref ? lc $read_pref->mode : 'primary';
    my $method =
        $self->type eq "Single"  ? '_find_available_server'
      : $self->type eq "Sharded" ? '_find_readable_mongos_server'
      :                            "_find_${mode}_server";

    if ( $mode eq 'primary' && $self->current_primary && $self->next_scan_time > time() )
    {
        my $link = $self->_get_server_link( $self->current_primary, $method );
        return $link if $link;
    }

    while ( my $server = $self->_selection_timeout( $method, $read_pref ) ) {
        my $link = $self->_get_server_link( $server, $method, $read_pref );
        if ($link) {
            $self->_set_current_primary($server)
              if $mode eq 'primary'
              && ( $self->type eq "ReplicaSetWithPrimary"
                || 1 == keys %{ $self->servers } );
            return $link;
        }
    }

    my $rp = $read_pref ? $read_pref->as_string : 'primary';

    MongoDB::SelectionError->throw(
        message => "No readable server available for matching read preference $rp. MongoDB server status:\n"
          . $self->_status_string,
        _maybe_get_txn_error_labels_and_unpin_from( $op ),
    );
}

sub get_specific_link {
    my ( $self, $address, $op ) = @_;
    $self->_check_for_uri_changes;

    my $server = $self->servers->{$address};
    if ( $server && ( my $link = $self->_get_server_link($server) ) ) {
        return $link;
    }
    else {
        MongoDB::SelectionError->throw(
            message => "Server $address is no longer available",
            _maybe_get_txn_error_labels_and_unpin_from( $op ),
        );
    }
}

sub get_writable_link {
    my ( $self, $op ) = @_;
    $self->_check_for_uri_changes;

    my $method =
      ( $self->type eq "Single" || $self->type eq "Sharded" )
      ? '_find_available_server'
      : "_find_primary_server";


    if ( $self->current_primary && $self->next_scan_time > time() ) {
        my $link = $self->_get_server_link( $self->current_primary, $method );
        return $link if $link;
    }

    while ( my $server = $self->_selection_timeout($method) ) {
        my $link = $self->_get_server_link( $server, $method );
        if ($link) {
            $self->_set_current_primary($server)
              if $self->type eq "ReplicaSetWithPrimary"
              || 1 == keys %{ $self->servers };
            return $link;
        }
    }

    MongoDB::SelectionError->throw(
        message => "No writable server available.  MongoDB server status:\n" . $self->_status_string,
        _maybe_get_txn_error_labels_and_unpin_from( $op ),
    );
}

# Marking a server unknown from outside the topology indicates an operational
# error, so the last scan is set to EPOCH so that the next scan won't wait for
# the scanning cooldown.
sub mark_server_unknown {
    my ( $self, $server, $error, $no_cooldown ) = @_;
    $self->_reset_address_to_unknown( $server->address, $error, $no_cooldown // EPOCH );
    return;
}

sub mark_stale {
    my ($self) = @_;
    $self->_set_stale(1);
    return;
}

sub scan_all_servers {
    my ($self, $force) = @_;

    my ( $next, @ordinary, @to_check );
    my $start_time = time;
    my $cooldown_time = $force ? $start_time : $start_time - COOLDOWN_SECS;

    # anything not updated since scan start is eligible for a check; when all servers
    # are updated, the loop terminates; Unknown servers aren't checked if
    # they are in the cooldown window since we don't want to wait the connect
    # timeout each attempt when they are unlikely to have changed status
    while (1) {
        @to_check =
          grep {
            $_->type eq 'Unknown'
              ? !$_->updated_since($cooldown_time)
              : !$_->updated_since($start_time)
          } $self->all_servers;

        last unless @to_check;

        if ( $next = first { $_->type eq 'RSPrimary' } @to_check ) {
            $self->check_address( $next->address );
        }
        elsif ( $next = first { $_->type eq 'PossiblePrimary' } @to_check ) {
            $self->check_address( $next->address );
        }
        elsif ( @ordinary = grep { $_->type ne 'Unknown' && $_->type ne 'RSGhost' } @to_check ) {
            $self->_check_oldest_server(@ordinary);
        }
        else {
            $self->_check_oldest_server(@to_check);
        }
    }

    my $now = time();
    $self->_set_last_scan_time( $now );
    $self->_set_next_scan_time( $now + $self->heartbeat_frequency_sec );
    $self->_set_stale( 0 );
    $self->_check_wire_versions;
    return;
}

sub status_struct {
    my ($self) = @_;
    my $status = { topology_type => $self->type, };
    $status->{replica_set_name} = $self->replica_set_name if $self->replica_set_name;

    # convert from [sec, microsec] array to floating point
    $status->{last_scan_time} = $self->last_scan_time;

    my $rtt_hash = $self->rtt_ewma_sec;
    my $ss = $status->{servers} = [];
    for my $server ( $self->all_servers ) {
        my $addr = $server->address;
        my $server_struct = $server->status_struct;
        if ( defined $rtt_hash->{$addr} ) {
            $server_struct->{ewma_rtt_sec} = $rtt_hash->{$addr};
        }
        push @$ss, $server_struct;
    }
    return $status;
}

#--------------------------------------------------------------------------#
# private methods
#--------------------------------------------------------------------------#

sub _add_address_as_unknown {
    my ( $self, $address, $last_update, $error ) = @_;
    $error = $error ? "$error" : "";
    $error =~ s/ at \S+ line \d+.*//ms;

    $self->publish_server_opening($address)
      if $self->monitoring_callback;

    return $self->servers->{$address} = MongoDB::_Server->new(
        address          => $address,
        last_update_time => $last_update || EPOCH,
        error            => $error,
    );
}

sub _check_for_primary {
    my ($self) = @_;
    if ( 0 == $self->_primaries ) {
        $self->_set_type('ReplicaSetNoPrimary');
        $self->_clear_current_primary;
        return 0;
    }
    return 1;
}

sub _check_oldest_server {
    my ( $self, @to_check ) = @_;

    my @ordered =
      map { $_->[0] }
      sort { $a->[1] <=> $b->[1] || rand() <=> rand() } # random if equal
      map { [ $_, $_->last_update_time ] }         # ignore partial secs
      @to_check;

    $self->check_address( $ordered[0]->address );

    return;
}

my $max_int32 = 2147483647;

sub _check_wire_versions {
    my ($self) = @_;

    my $compat = 1;
    my $min_seen = $max_int32;
    my $max_seen = 0;
    for my $server ( grep { $_->is_available } $self->all_servers ) {
        my ( $server_min_wire_version, $server_max_wire_version ) =
          @{ $server->is_master }{qw/minWireVersion maxWireVersion/};

        # set to 0 as could be undefined. 0 is the equivalent to missing, and
        # also kept as 0 for legacy compatibility.
        $server_max_wire_version = 0 unless defined $server_max_wire_version;
        $server_min_wire_version = 0 unless defined $server_min_wire_version;

        if ( $server_min_wire_version > $self->max_wire_version
          || $server_max_wire_version < $self->min_wire_version ) {
            $compat = 0;
            push @{ $self->_incompatible_servers }, $server;
        }

        $min_seen = $server_max_wire_version if $server_max_wire_version < $min_seen;
        $max_seen = $server_max_wire_version if $server_max_wire_version > $max_seen;
    }
    $self->_set_is_compatible($compat);
    $self->_set_wire_version_floor($min_seen);
    $self->_set_wire_version_ceil($max_seen);

    return;
}

sub _update_ls_timeout_minutes {
    my ( $self, $new_server ) = @_;

    my @data_bearing_servers = grep { $_->is_data_bearing } $self->all_servers;
    my $timeout = min map {
        # use -1 as a flag to prevent undefined warnings
        defined $_->logical_session_timeout_minutes
          ? $_->logical_session_timeout_minutes
          : -1
    } @data_bearing_servers;
    # min will return undef if the array is empty
    if ( defined $timeout && $timeout < 0 ) {
        $timeout = undef;
    }
    $self->_set_logical_session_timeout_minutes( $timeout );
    return;
}

sub _supports_sessions {
    my ( $self ) = @_;

    $self->scan_all_servers if $self->stale;

    my @servers = $self->all_servers;
    return if @servers == 1 && $servers[0]->type eq 'Standalone';

    return defined $self->logical_session_timeout_minutes;
}

sub _supports_transactions {
    my ( $self ) = @_;

    return unless $self->_supports_sessions;
    return $self->_supports_mongos_pinning_transactions if $self->type eq 'Sharded';
    return if $self->wire_version_ceil < 7;
    return 1;
}

# Seperated out so can be used in dispatch logic
sub _supports_mongos_pinning_transactions {
    my ( $self ) = @_;

    # Separated out so that it doesnt return 1 for wire version 8 non sharded
    return if $self->wire_version_ceil < 8;
    # This extra sharded check is required so this test can be standalone
    return if $self->type ne 'Sharded';
    return 1;
}

sub _check_staleness_compatibility {
    my ($self, $read_pref) = @_;
    my $max_staleness_sec = $read_pref ? $read_pref->max_staleness_seconds : -1;

    if ( $max_staleness_sec > 0 ) {
        if ( $self->wire_version_floor < 5 ) {
            MongoDB::ProtocolError->throw(
                "Incompatible wire protocol version. You tried to use max_staleness_seconds with one or more servers that don't support it."
            );
        }

        if (
            ( $self->type eq "ReplicaSetWithPrimary" || $self->type eq "ReplicaSetNoPrimary" )
            && $max_staleness_sec < max( SMALLEST_MAX_STALENESS_SEC,
                $self->heartbeat_frequency_sec + IDLE_WRITE_PERIOD_SEC
            )
          )
        {
            MongoDB::UsageError->throw(
                "max_staleness_seconds must be at least 90 seconds and at least heartbeat_frequency (in secs) + 10 secs"
            );
        }
    }

    return;
}

sub _dump {
    my ($self) = @_;
    print $self->_status_string . "\n";
}

sub _eligible {
    my ( $self, $read_pref, @candidates ) = @_;

    # must filter on max staleness first, so that the remaining servers
    # are checked against the list of tag_sets
    if ( $read_pref->max_staleness_seconds > 0 ) {
        @candidates = $self->_filter_fresh_servers($read_pref, @candidates );
        return unless @candidates;
    };

    # given a tag set list, if a tag set matches at least one
    # candidate, then all candidates matching that tag set are eligible
    if ( ! $read_pref->has_empty_tag_sets ) {
        for my $ts ( @{ $read_pref->tag_sets } ) {
            if ( my @ts_candidates = grep { $_->matches_tag_set($ts) } @candidates ) {
                return @ts_candidates;
            }
        }
        return;
    }

    return @candidates;
}

sub _filter_fresh_servers {
    my ($self, $read_pref, @candidates) = @_;

    # all values should be floating point seconds
    my $max_staleness_sec = $read_pref->max_staleness_seconds;
    my $heartbeat_frequency_sec = $self->heartbeat_frequency_sec;

    if ( $self->type eq 'ReplicaSetWithPrimary' ) {
        my ($primary) = $self->_primaries;

        # all values should be floating point seconds
        my $p_last_write_date = $primary->last_write_date;
        my $p_last_update_time = $primary->last_update_time;

        return map { $_->[0] }
          grep { $_->[1] <= $max_staleness_sec }
          map {
            [
                $_,
                $p_last_write_date
                  + ( $_->last_update_time - $p_last_update_time )
                  - $_->last_write_date
                  + $heartbeat_frequency_sec
            ]
          } @candidates;
    }
    else {
        my ($smax) = map { $_->[0] }
          sort { $b->[1] <=> $a->[1] }
          map { [ $_, $_->last_write_date ] } $self->_secondaries;
        my $smax_last_write_date = $smax->last_write_date;

        return map { $_->[0] }
          grep     { $_->[1] <= $max_staleness_sec }
          map {
            [ $_, $smax_last_write_date - $_->last_write_date + $heartbeat_frequency_sec ]
          } @candidates;
    }
}

# This works for reads and writes; for writes, $read_pref will be undef
sub _find_available_server {
    my ( $self, $read_pref, @candidates ) = @_;
    $self->_check_staleness_compatibility($read_pref) if $read_pref;
    push @candidates, $self->all_servers unless @candidates;
    my $selector = $self->server_selector;
    return $self->_get_server_in_latency_window(
      [ grep { $_->is_available }
          $selector ? $selector->(@candidates) : @candidates ]
    );
}

# This uses read preference to check for max staleness compatibility in
# mongos, but otherwise read preference is ignored (mongos will pass it on)
sub _find_readable_mongos_server {
    my ( $self, $read_pref, @candidates ) = @_;
    $self->_check_staleness_compatibility($read_pref);
    push @candidates, $self->all_servers unless @candidates;
    my $selector = $self->server_selector;
    return $self->_get_server_in_latency_window(
      [ grep { $_->is_available }
          $selector ? $selector->(@candidates) : @candidates ]
    );
}

sub _find_nearest_server {
    my ( $self, $read_pref, @candidates ) = @_;
    $self->_check_staleness_compatibility($read_pref);
    push @candidates, ( $self->_primaries, $self->_secondaries ) unless @candidates;
    my @suitable = $self->_eligible( $read_pref, @candidates );
    my $selector = $self->server_selector;
    return $self->_get_server_in_latency_window(
        [ $selector ? $selector->(@suitable) : @suitable ]
    );
}

sub _find_primary_server {
    my ( $self, undef, @candidates ) = @_;
    return $self->current_primary
      if $self->current_primary;
    push @candidates, $self->all_servers unless @candidates;
    return first { $_->is_writable } @candidates;
}

sub _find_primarypreferred_server {
    my ( $self, $read_pref, @candidates ) = @_;
    $self->_check_staleness_compatibility($read_pref);
    return $self->_find_primary_server(@candidates)
      || $self->_find_secondary_server( $read_pref, @candidates );
}

sub _find_secondary_server {
    my ( $self, $read_pref, @candidates ) = @_;
    $self->_check_staleness_compatibility($read_pref);
    push @candidates, $self->_secondaries unless @candidates;
    my @suitable = $self->_eligible( $read_pref, @candidates );
    my $selector = $self->server_selector;
    return $self->_get_server_in_latency_window(
        [ $selector ? $selector->(@suitable) : @suitable ]
    );
}

sub _find_secondarypreferred_server {
    my ( $self, $read_pref, @candidates ) = @_;
    $self->_check_staleness_compatibility($read_pref);
    return $self->_find_secondary_server( $read_pref, @candidates )
      || $self->_find_primary_server(@candidates);
}

sub _get_server_in_latency_window {
    my ( $self, $servers ) = @_;
    return unless @$servers;
    return $servers->[0] if @$servers == 1;

    # order servers by RTT EWMA
    my $rtt_hash = $self->rtt_ewma_sec;
    my @sorted =
      sort { $a->{rtt} <=> $b->{rtt} }
      map { { server => $_, rtt => $rtt_hash->{ $_->address } } } @$servers;
    # lowest RTT is always in the windows
    my @in_window = shift @sorted;

    # add any other servers in window and return a random one
    my $max_rtt = $in_window[0]->{rtt} + $self->local_threshold_sec;
    push @in_window, grep { $_->{rtt} <= $max_rtt } @sorted;
    return $in_window[ int( rand(@in_window) ) ]->{server};
}

my $PRIMARY = MongoDB::ReadPreference->new;
my $PRIMARY_PREF = MongoDB::ReadPreference->new( mode => 'primaryPreferred' );

sub _ping_server {
    my ($self, $link) = @_;
    return eval {
        my $op = MongoDB::Op::_Command->_new(
            db_name             => 'admin',
            query               => [ping => 1],
            query_flags         => {},
            bson_codec          => $self->bson_codec,
            read_preference     => $PRIMARY_PREF,
            monitoring_callback => $self->monitoring_callback,
        );
        $op->execute( $link )->output;
    };
}


sub _get_server_link {
    my ( $self, $server, $method, $read_pref ) = @_;
    my $address = $server->address;
    my $link    = $self->links->{$address};

    # if no link, make a new connection or give up
    $link = $self->_initialize_link($address) unless $link && $link->connected;
    return unless $link;

    # for idle links, refresh the server and verify validity
    if ( time - $link->last_used > $self->socket_check_interval_sec ) {
        return $link if $self->_ping_server;
        $self->mark_server_unknown(
          $server, 'Lost connection with the server'
        );
        $self->check_address($address);

        # topology might have dropped the server
        $server = $self->servers->{$address}
          or return;

        my $fresh_link = $self->links->{$address};
        return $fresh_link if !$method;

        # verify selection criteria
        return $self->$method( $read_pref, $server ) ? $fresh_link : undef;
    }

    return $link;
}

sub _initialize_link {
    my ( $self, $address ) = @_;

    my $link = eval {
        MongoDB::_Link->new( %{$self->link_options}, address => $address )->connect;
    } or do {
        my $error = $@ || "Unknown error";
        # if connection failed, update topology with Unknown description
        $self->_reset_address_to_unknown( $address, $error );
    };

    return unless $link;

    # connection succeeded, so register link and get a server description
    $self->links->{$address} = $link;
    $self->_update_topology_from_link( $link, with_handshake => 1 );

    # after update, server might or might not exist in the topology;
    # if not, return nothing
    return unless my $server = $self->servers->{$address};

    # we have a link and the server is a valid member, so
    # try to authenticate; if authentication fails, all
    # servers are considered invalid and we throw an error
    if ( $self->type eq 'Single' || first { $_ eq $server->type } qw/Standalone Mongos RSPrimary RSSecondary/ ) {
        eval {
            $self->credential->authenticate($server, $link, $self->bson_codec);
            1;
        } or do {
            my $err = $@;
            my $msg = $err->$_isa("MongoDB::Error") ? $err->message : "$err";
            $self->_reset_address_to_unknown( $_->address, $err ) for $self->all_servers;
            MongoDB::AuthError->throw("Authentication to $address failed: $msg");
        };
    }

    return $link;
}

sub _primaries {
    return grep { $_->type eq 'RSPrimary' } $_[0]->all_servers;
}

sub _remove_address {
    my ( $self, $address ) = @_;
    if ( $self->current_primary &&  $self->current_primary->address eq $address ) {
        $self->_clear_current_primary;
    }
    delete $self->$_->{$address} for qw/servers links rtt_ewma_sec/;
    $self->publish_server_closing( $address )
      if $self->monitoring_callback;
    return;
}

sub _remove_server {
    my ( $self, $server ) = @_;
    $self->_remove_address( $server->address );
    return;
}

sub _reset_address_to_unknown {
    my ( $self, $address, $error, $update_time ) = @_;
    $update_time //= time;

    $self->_remove_address($address);
    my $desc = $self->_add_address_as_unknown( $address, $update_time, $error );
    $self->_update_topology_from_server_desc($address, $desc);

    return;
}

sub _secondaries {
    return grep { $_->type eq 'RSSecondary' } $_[0]->all_servers;
}

sub _status_string {
    my ($self) = @_;
    my $status = '';
    if ( $self->type =~ /^Replica/ ) {
        $status .= sprintf( "Topology type: %s; Set name: %s, Member status:\n",
            $self->type, $self->replica_set_name );
    }
    else {
        $status .= sprintf( "Topology type: %s; Member status:\n", $self->type );
    }

    $status .= join( "\n", map { "  $_" } map { $_->status_string } $self->all_servers ) . "\n";
    return $status;
}

# this implements the server selection timeout around whatever actual method
# is used for returning a link
sub _selection_timeout {
    my ( $self, $method, $read_pref ) = @_;

    my $start_time = my $loop_end_time = time();
    my $max_time = $start_time + $self->server_selection_timeout_sec;

    if ( $self->next_scan_time < $start_time ) {
        $self->_set_stale(1);
    }

    while (1) {
        if ( $self->stale ) {
            my $scan_ready_time = $self->last_scan_time + MIN_HEARTBEAT_FREQUENCY_SEC;

            # if not enough time left to wait to check; then caller throws error
            return if !$self->server_selection_try_once && $scan_ready_time > $max_time;

            # loop_end_time is a proxy for time() to avoid overhead
            my $sleep_time = $scan_ready_time - $loop_end_time;

            usleep( 1e6 * $sleep_time ) if $sleep_time > 0;

            $self->scan_all_servers;
        }

        unless ( $self->is_compatible ) {
            $self->_set_stale(1);
            my $error_string = '';
            for my $server ( @{ $self->_incompatible_servers } ) {
                my $min_wire_ver = $server->is_master->{minWireVersion};
                my $max_wire_ver = $server->is_master->{maxWireVersion};
                my $host         = $server->address;
                if ( $min_wire_ver > $self->max_wire_version ) {
                    $error_string .= sprintf(
                        "Server at %s requires wire version %d, but this version of %s only supports up to %d.\n",
                        $host,
                        $min_wire_ver,
                        'Perl MongoDB Driver',
                        $self->max_wire_version
                    );
                } else {
                    $error_string .= sprintf(
                        "Server at %s reports wire version %d, but this version of %s requires at least %d (MongoDB %s).\n",
                        $host,
                        $max_wire_ver,
                        'Perl MongoDB Driver',
                        $self->min_wire_version,
                        $self->min_server_version,
                    );
                }
            }
            $self->_set_compatibility_error($error_string);
            MongoDB::ProtocolError->throw( $error_string );
        }

        my $server = $self->$method($read_pref);

        return $server if $server;

        $self->_set_stale(1);
        $loop_end_time = time();

        if ( $self->server_selection_try_once ) {
            # if already tried once; then caller throws error
            return if $self->last_scan_time > $start_time;
        }
        else {
            # if selection timed out; then caller throws error
            return if $loop_end_time > $max_time;
        }
    }
}

sub _generate_ismaster_request {
    my ( $self, $link, $should_perform_handshake ) = @_;
    my @opts;
    if ($should_perform_handshake) {
        push @opts, client => $self->handshake_document;
        if ( $self->credential->mechanism eq 'DEFAULT' ) {
            my $db_user = join( ".", map { $self->credential->$_ } qw/source username/ );
            push @opts, saslSupportedMechs => $db_user;
        }
        if (@{ $self->compressors }) {
            push @opts, compression => $self->compressors;
        }
    }
    if ( $link->supports_clusterTime && defined $self->cluster_time ) {
        push @opts, '$clusterTime' => $self->cluster_time;
    }

    return [ ismaster => 1, @opts ];
}

sub _update_topology_from_link {
    my ( $self, $link, %opts ) = @_;

    $self->publish_server_heartbeat_started( $link )
      if $self->monitoring_callback;

    my $start_time = time;
    my $is_master = eval {
        my $op = MongoDB::Op::_Command->_new(
            db_name             => 'admin',
            query               => $self->_generate_ismaster_request( $link, $opts{with_handshake} ),
            query_flags         => {},
            bson_codec          => $self->bson_codec,
            read_preference     => $PRIMARY,
            monitoring_callback => $self->monitoring_callback,
        );
        # just for this command, use connect timeout as socket timeout;
        # this violates encapsulation, but requires less API modification
        # to support this specific exception to the socket timeout
        local $link->{socket_timeout} = $link->{connect_timeout};
        $op->execute( $link )->output;
    };
    if ( my $e = $@ ) {
        my $end_time_fail = time;
        my $rtt_sec_fail = $end_time_fail - $start_time;
        $self->publish_server_heartbeat_failed( $link, $rtt_sec_fail, $e )
          if $self->monitoring_callback;
        if ($e->$_isa("MongoDB::DatabaseError") && $e->code == USER_NOT_FOUND ) {
            MongoDB::AuthError->throw("mechanism negotiation error: $e");
        }
        warn "During MongoDB topology update for @{[$link->address]}: $e"
            if WITH_ASSERTS;
        $self->_reset_address_to_unknown( $link->address, $e );
        # retry a network error if server was previously known to us
        if (    $e->$_isa("MongoDB::NetworkError")
            and $link->server
            and $link->server->type ne 'Unknown'
            and $link->server->type ne 'PossiblePrimary' )
        {
            # the earlier reset to unknown avoids us reaching this branch again
            # and recursing forever
            $self->check_address( $link->address );
        }
        return;
    };

    return unless $is_master;

    if ( my $cluster_time = $is_master->{'$clusterTime'} ) {
        $self->update_cluster_time($cluster_time);
    }

    my $end_time = time;
    my $rtt_sec = $end_time - $start_time;
    # Protect against clock skew
    $rtt_sec = 0 if $rtt_sec < 0;

    $self->publish_server_heartbeat_succeeded( $link, $rtt_sec, $is_master )
      if $self->monitoring_callback;

    my $new_server = MongoDB::_Server->new(
        address          => $link->address,
        last_update_time => $end_time,
        rtt_sec           => $rtt_sec,
        is_master        => $is_master,
        compressor       => $self->_construct_compressor($is_master),
    );

    $self->_update_topology_from_server_desc( $link->address, $new_server );

    return;
}

# find suitable compressor
#
# implemented here because the result is based on the specified
# order of compressors combined with the list of server supported
# compressors
sub _construct_compressor {
    my ($self, $is_master) = @_;

    my @supported = @{ ($is_master || {})->{compression} || [] }
        or return undef; ## no critic

    for my $name (@{ $self->compressors }) {
        if (grep { $name eq $_ } @supported) {
            return MongoDB::_Protocol::get_compressor($name, {
                zlib_compression_level => $self->zlib_compression_level,
            });
        }
    }

    return undef; ## no critic
}

sub _update_topology_from_server_desc {
    my ( $self, $address, $new_server ) = @_;

    # ignore spurious result not in the set; this isn't strictly necessary
    # for single-threaded operation, but spec tests expect it and if we
    # have async monitoring in the future, late responses could come back
    # after a server has been removed
    return unless $self->servers->{$address};

    $self->publish_old_topology_desc( $address, $new_server )
      if $self->monitoring_callback;

    $self->_update_ewma( $address, $new_server );

    # must come after ewma update
    $self->servers->{$address} = $new_server;

    my $method = "_update_" . $self->type;

    $self->$method( $address, $new_server );

    # if link is still around, tag it with server specifics
    $self->_update_link_metadata( $address, $new_server );

    $self->_update_ls_timeout_minutes( $new_server );

    $self->publish_new_topology_desc if $self->monitoring_callback;

    return $new_server;
}

sub _update_ewma {
    my ( $self, $address, $new_server ) = @_;

    if ( $new_server->type eq 'Unknown' ) {
        delete $self->rtt_ewma_sec->{$address};
    }
    else {
        my $old_avg = $self->rtt_ewma_sec->{$address};
        my $alpha   = $self->ewma_alpha;
        my $rtt_sec  = $new_server->rtt_sec;
        $self->rtt_ewma_sec->{$address} =
          defined($old_avg) ? ( $alpha * $rtt_sec + ( 1 - $alpha ) * $old_avg ) : $rtt_sec;
    }

    return;
}

sub _update_link_metadata {
    my ( $self, $address, $server ) = @_;

    # if the link didn't get dropped from the topology during the update, we
    # attach the server so the link knows where it came from
    if ( $self->links->{$address} ) {
        $self->links->{$address}->set_metadata($server);
    }

    return;
}

sub _update_rs_with_primary_from_member {
    my ( $self, $new_server ) = @_;

    if (  !$self->servers->{ $new_server->address }
        || $self->replica_set_name ne $new_server->set_name )
    {
        $self->_remove_server($new_server);
    }

    # require 'me' that matches expected address.
    # check is case insensitive
    if ( $new_server->me && lc $new_server->me ne $new_server->address ) {
        $self->_remove_server($new_server);
        $self->_check_for_primary;
        return;
    }

    if ( ! $self->_check_for_primary ) {

        # flag possible primary to amend scanning order
        my $primary = $new_server->primary;
        if (   length($primary)
            && $self->servers->{$primary}
            && $self->servers->{$primary}->type eq 'Unknown' )
        {
            $self->servers->{$primary}->_set_type('PossiblePrimary');
        }
    }

    return;
}

sub _update_rs_with_primary_from_primary {
    my ( $self, $new_server ) = @_;

    if ( !length $self->replica_set_name ) {
        $self->_set_replica_set_name( $new_server->set_name );
    }
    elsif ( $self->replica_set_name ne $new_server->set_name ) {
        # We found a primary but it doesn't have the setName
        # provided by the user or previously discovered
        $self->_remove_server($new_server);
        return;
    }

    my $election_id = $new_server->is_master->{electionId};
    my $set_version = $new_server->is_master->{setVersion};
    my $max_election_id = $self->max_election_id;
    my $max_set_version = $self->max_set_version;

    if ( defined $set_version && defined $election_id ) {
        if (
               defined $max_election_id
            && defined $max_set_version
            && (
                $max_set_version > $set_version
                || (   $max_set_version == $set_version
                    && "$max_election_id" gt "$election_id" )
            )
          )
        {
            # stale primary

            $self->_remove_address( $new_server->address );
            $self->_add_address_as_unknown( $new_server->address );
            $self->_check_for_primary;
            return;
        }
        $self->_set_max_election_id( $election_id );
    }

    if ( defined $set_version
        && ( !defined $max_set_version || $set_version > $max_set_version ) )
    {
        $self->_set_max_set_version($set_version);
    }

    # possibly invalidate an old primary (even if more than one!)
    for my $old_primary ( $self->_primaries ) {
        if ( $old_primary->address ne $new_server->address ) {
            $self->_reset_address_to_unknown(
                $old_primary->address,
                "no longer primary; update needed",
                $old_primary->last_update_time
            );
        }
    }

    # unknown set members need to be added to the topology
    my %set_members =
      map { $_ => undef } map { @{ $new_server->$_ } } qw/hosts passives arbiters/;

    $self->_add_address_as_unknown($_)
      for grep { !exists $self->servers->{$_} } keys %set_members;

    # topology servers no longer in the set need to be removed
    $self->_remove_address($_)
      for grep { !exists $set_members{$_} } keys %{ $self->servers };

    return;
}

sub _update_rs_without_primary {
    my ( $self, $new_server ) = @_;

    if ( !length $self->replica_set_name ) {
        $self->_set_replica_set_name( $new_server->set_name );
    }
    elsif ( $self->replica_set_name ne $new_server->set_name ) {
        $self->_remove_server($new_server);
        return;
    }

    # unknown set members need to be added to the topology
    my %set_members =
      map { $_ => undef } map { @{ $new_server->$_ } } qw/hosts passives arbiters/;

    $self->_add_address_as_unknown($_)
      for grep { !exists $self->servers->{$_} } keys %set_members;

    # require 'me' that matches expected address
    if ( $new_server->me && $new_server->me ne $new_server->address ) {
        $self->_remove_server($new_server);
        return;
    }

    # flag possible primary to amend scanning order
    my $primary = $new_server->primary;
    if (   length($primary)
        && $self->servers->{$primary}
        && $self->servers->{$primary}->type eq 'Unknown' )
    {
        $self->servers->{$primary}->_set_type('PossiblePrimary');
    }

    return;
}

#--------------------------------------------------------------------------#
# update methods by topology types: behavior in each depends on new server
# type received
#--------------------------------------------------------------------------#

sub _update_ReplicaSetNoPrimary {
    my ( $self, $address, $new_server ) = @_;

    my $server_type = $new_server->type;

    if ( $server_type eq 'RSPrimary' ) {
        $self->_set_type('ReplicaSetWithPrimary');
        $self->_update_rs_with_primary_from_primary($new_server);
        # topology changes might have removed all primaries
        $self->_check_for_primary;
    }
    elsif ( grep { $server_type eq $_ } qw/RSSecondary RSArbiter RSOther/ ) {
        $self->_update_rs_without_primary($new_server);
    }
    elsif ( grep { $server_type eq $_ } qw/Standalone Mongos/ ) {
        $self->_remove_server($new_server);
    }
    else {
        # Unknown or RSGhost are no-ops
    }

    return;
}

sub _update_ReplicaSetWithPrimary {
    my ( $self, $address, $new_server ) = @_;

    my $server_type = $new_server->type;

    if ( $server_type eq 'RSPrimary' ) {
        $self->_update_rs_with_primary_from_primary($new_server);
    }
    elsif ( grep { $server_type eq $_ } qw/RSSecondary RSArbiter RSOther/ ) {
        $self->_update_rs_with_primary_from_member($new_server);
    }
    elsif ( grep { $server_type eq $_ } qw/Unknown Standalone Mongos/ ) {
        $self->_remove_server($new_server)
          unless $server_type eq 'Unknown';
    }
    else {
        # RSGhost is no-op
    }

    # topology changes might have removed all primaries
    $self->_check_for_primary;

    return;
}

sub _update_Sharded {
    my ( $self, $address, $new_server ) = @_;

    my $server_type = $new_server->type;

    if ( grep { $server_type eq $_ } qw/Unknown Mongos/ ) {
        # no-op
    }
    else {
        $self->_remove_server($new_server);
    }

    return;
}

sub _update_Single {
    my ( $self, $address, $new_server ) = @_;
    # Per the spec, TopologyType Single never changes type or membership
    return;
}

# Direct mode is like Unknown, except that it switches only between Sharded
# or Single based on the response.
sub _update_Direct {
    my ( $self, $address, $new_server ) = @_;

    my $server_type = $new_server->type;

    if ( $server_type eq 'Mongos' ) {
        $self->_set_type('Sharded');
        return;
    }

    $self->_set_type('Single');
    return;
}

sub _update_Unknown {
    my ( $self, $address, $new_server ) = @_;

    my $server_type = $new_server->type;

    # Starting from topology type 'unknown', a standalone server when we
    # were given multiple seeds must be a replica set member in maintenance
    # mode so we drop it and will rediscover it later.
    if ( $server_type eq 'Standalone' ) {
        if ( $self->number_of_seeds > 1 ) {
            $self->_remove_address($address);
        }
        else {
            $self->_set_type('Single');
        }
        return;
    }

    if ( $server_type eq 'Mongos' ) {
        $self->_set_type('Sharded');
        return;
    }

    if ( $server_type eq 'RSPrimary' ) {
        $self->_set_type('ReplicaSetWithPrimary');
        $self->_update_rs_with_primary_from_primary($new_server);
        # topology changes might have removed all primaries
        $self->_check_for_primary;
    }
    elsif ( grep { $server_type eq $_ }  qw/RSSecondary RSArbiter RSOther/ ) {
        $self->_set_type('ReplicaSetNoPrimary');
        $self->_update_rs_without_primary($new_server);
    }
    else {
        # Unknown or RSGhost are no-ops
    }

    return;
}

1;

# vim: ts=4 sts=4 sw=4 et:
