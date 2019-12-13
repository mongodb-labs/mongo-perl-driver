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
package MongoDB::_Server;

use version;
our $VERSION = 'v2.2.2';

use Moo;
use MongoDB::_Types qw(
    Boolish
    NonNegNum
    HostAddress
    ServerType
    HostAddressList
);
use Types::Standard qw(
    InstanceOf
    HashRef
    Str
    Num
    Maybe
);
use List::Util qw/first/;
use Time::HiRes qw/time/;
use namespace::clean -except => 'meta';

# address: the hostname or IP, and the port number, that the client connects
# to. Note that this is not the server's ismaster.me field, in the case that
# the server reports an address different from the address the client uses.

has address => (
    is       => 'ro',
    isa      => HostAddress,
    required => 1,
);

# lastUpdateTime: when this server was last checked. Default "infinity ago".

has last_update_time => (
    is       => 'ro',
    isa      => Num, # floating point time
    required => 1,
);

# error: information about the last error related to this server. Default null.

has error => (
    is      => 'ro',
    isa     => Str,
    default => '',
);

# roundTripTime: the duration of the ismaster call. Default null.

has rtt_sec => (
    is      => 'ro',
    isa     => NonNegNum,
    default => 0,
);

# is_master: hashref returned from an is_master command

has is_master => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

# compressor: hashref with id/callback values for used compression

has compressor => (
    is => 'ro',
    isa => Maybe[HashRef],
);

# type: a ServerType enum value. Default Unknown.  Definitions from the Server
# Discovery and Monitoring Spec:
# - Unknown	Initial, or after a network error or failed ismaster call, or "ok: 1"
#   not in ismaster response.
# - Standalone	No "msg: isdbgrid", no setName, and no "isreplicaset: true".
# - Mongos	"msg: isdbgrid".
# - RSPrimary	"ismaster: true", "setName" in response.
# - RSSecondary	"secondary: true", "setName" in response.
# - RSArbiter	"arbiterOnly: true", "setName" in response.
# - RSOther	"setName" in response, "hidden: true" or not primary, secondary, nor arbiter.
# - RSGhost	"isreplicaset: true" in response.
# - PossiblePrimary	Not yet checked, but another member thinks it is the primary.

has type => (
    is      => 'lazy',
    isa     => ServerType,
    builder => '_build_type',
    writer  => '_set_type',
);

sub _build_type {
    my ($self) = @_;
    my $is_master = $self->is_master;
    if ( !$is_master->{ok} ) {
        return 'Unknown';
    }
    elsif ( $is_master->{msg} && $is_master->{msg} eq 'isdbgrid' ) {
        return 'Mongos';
    }
    elsif ( $is_master->{isreplicaset} ) {
        return 'RSGhost';
    }
    elsif ( exists $is_master->{setName} ) {
        return
            $is_master->{ismaster}    ? return 'RSPrimary'
          : $is_master->{hidden}      ? return 'RSOther'
          : $is_master->{secondary}   ? return 'RSSecondary'
          : $is_master->{arbiterOnly} ? return 'RSArbiter'
          :                             'RSOther';
    }
    else {
        return 'Standalone';
    }
}

# hosts, passives, arbiters: Sets of addresses. This server's opinion of the
# replica set's members, if any. Default empty. The client monitors all three
# types of servers in a replica set.

for my $s (qw/hosts passives arbiters/) {
    has $s => (
        is      => 'lazy',
        isa     => HostAddressList,
        builder => "_build_$s",
    );

    no strict 'refs'; ## no critic
    *{"_build_$s"} = sub {
        [ map { lc $_ } ( @{ $_[0]->is_master->{$s} || [] } ) ];
    };
}


# address configured as part of replica set: string or null. Default null.

has me => (
    is      => 'lazy',
    isa     => Str,
    builder => "_build_me",
);

sub _build_me {
    my ($self) = @_;
    return $self->is_master->{me} || '';
}

# setName: string or null. Default null.

has set_name => (
    is      => 'lazy',
    isa     => Str,
    builder => "_build_set_name",
);

sub _build_set_name {
    my ($self) = @_;
    return $self->is_master->{setName} || '';
}

# primary: an address. This server's opinion of who the primary is. Default
# null.

has primary => (
    is      => 'lazy',
    isa     => Str,           # not HostAddress -- might be empty string
    builder => "_build_primary",
);

sub _build_primary {
    my ($self) = @_;
    return $self->is_master->{primary} || '';
}

# tags: (a tag set) map from string to string. Default empty.

has tags => (
    is      => 'lazy',
    isa     => HashRef,
    builder => "_build_tags",
);

sub _build_tags {
    my ($self) = @_;
    return $self->is_master->{tags} || {};
}

# last_write_date: for replica set and wire version 5+ (converted to
# seconds)
has last_write_date => (
    is      => 'lazy',
    isa     => Num,
    builder => "_build_last_write_date",
);

sub _build_last_write_date {
    my ($self) = @_;
    return 0 unless exists $self->is_master->{lastWrite}{lastWriteDate};
    return $self->is_master->{lastWrite}{lastWriteDate}->epoch;
}

has is_available => (
    is      => 'lazy',
    isa     => Boolish,
    builder => "_build_is_available",
);

sub _build_is_available {
    my ($self) = @_;
    return $self->type ne 'Unknown' && $self->type ne 'PossiblePrimary';
}

has is_readable => (
    is      => 'lazy',
    isa     => Boolish,
    builder => "_build_is_readable",
);

# any of these can take reads. Topologies will screen inappropriate
# ones out. E.g. "Standalone" won't be found in a replica set topology.
sub _build_is_readable {
    my ($self) = @_;
    my $type = $self->type;
    return !! grep { $type eq $_ } qw/Standalone RSPrimary RSSecondary Mongos/;
}

has is_writable => (
    is      => 'lazy',
    isa     => Boolish,
    builder => "_build_is_writable",
);

# any of these can take writes. Topologies will screen inappropriate
# ones out. E.g. "Standalone" won't be found in a replica set topology.
sub _build_is_writable {
    my ($self) = @_;
    my $type = $self->type;
    return !! grep { $type eq $_ } qw/Standalone RSPrimary Mongos/;
}

has is_data_bearing => (
    is => 'lazy',
    isa => Boolish,
    builder => "_build_is_data_bearing",
);

sub _build_is_data_bearing {
    my ( $self ) = @_;
    my $type = $self->type;
    return !! grep { $type eq $_ } qw/Standalone RSPrimary RSSecondary Mongos/;
}

# logicalSessionTimeoutMinutes can be not set by a client
has logical_session_timeout_minutes => (
    is => 'lazy',
    isa => Maybe [NonNegNum],
    builder => "_build_logical_session_timeout_minutes",
);

sub _build_logical_session_timeout_minutes {
    my ( $self ) = @_;
    return $self->is_master->{logicalSessionTimeoutMinutes} || undef;
}

sub updated_since {
    my ( $self, $time ) = @_;
    return( ($self->last_update_time - $time) > 0 );
}

# check if server matches a single tag set (NOT a tag set list)
sub matches_tag_set {
    my ( $self, $ts ) = @_;
    no warnings 'uninitialized'; # let undef equal empty string without complaint

    my $tg = $self->tags;

    # check if ts is a subset of tg: if any tags in ts that aren't in tg or where
    # the tag values aren't equal mean ts is NOT a subset
    if ( !defined first { !exists( $tg->{$_} ) || $tg->{$_} ne $ts->{$_} } keys %$ts ) {
        return 1;
    }

    return;
}

sub status_string {
    my ($self) = @_;
    if ( my $err = $self->error ) {
        $err =~ tr[\n][ ];
        return
          sprintf( "%s (type: %s, error: %s)", $self->{address}, $self->{type}, $err);
    }
    else {
        return sprintf( "%s (type: %s)", map { $self->$_ } qw/address type/ );
    }
}

sub status_struct {
    my ($self) = @_;
    my $info = {
        address          => $self->address,
        type             => $self->type,
        last_update_time => $self->last_update_time,
    };
    $info->{error} = $self->error         if $self->error;
    $info->{tags}  = { %{ $self->tags } } if %{ $self->tags };
    return $info;
}


1;

# vim: ts=4 sts=4 sw=4 et:
