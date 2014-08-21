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

package MongoDB::_Server;

use version;
our $VERSION = 'v0.704.4.1';

use Moose;
use MongoDB::_Types;
use Time::HiRes qw/tv_interval/;
use namespace::clean -except => 'meta';

# address: the hostname or IP, and the port number, that the client connects
# to. Note that this is not the server's ismaster.me field, in the case that
# the server reports an address different from the address the client uses.

has address => (
    is       => 'ro',
    isa      => 'HostAddress',
    required => 1,
);

# lastUpdateTime: when this server was last checked. Default "infinity ago".

has last_update_time => (
    is       => 'ro',
    isa      => 'ArrayRef', # [ Time::HighRes::gettimeofday() ]
    required => 1,
);

# roundTripTime: the duration of the ismaster call. Default null.

has rtt_ms => (
    is      => 'ro',
    isa     => 'Num',
    default => 0,
);

# is_master: hashref returned from an is_master command

has is_master => (
    is      => 'ro',
    isa     => 'HashRef',
    default => sub { {} },
);

# type: a ServerType enum value. Default Unknown.  Definitions from the Cluster
# Monitoring Spec:
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
    is      => 'ro',
    isa     => 'ServerType',
    lazy    => 1,
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
        is      => 'ro',
        isa     => 'HostAddressList',
        lazy    => 1,
        builder => "_build_$s",
        coerce => 1,
    );

    no strict 'refs';
    *{"_build_$s"} = sub { $_[0]->is_master->{$s} || [] };
}

# setName: string or null. Default null.

has set_name => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    builder => "_build_set_name",
);

sub _build_set_name {
    my ($self) = @_;
    return $self->is_master->{setName} || '';
}

# primary: an address. This server's opinion of who the primary is. Default
# null.

has primary => (
    is      => 'ro',
    isa     => 'Str',           # not HostAddress -- might be empty string
    lazy    => 1,
    builder => "_build_primary",
);

sub _build_primary {
    my ($self) = @_;
    return $self->is_master->{primary} || '';
}

# tags: map from string to string. Default empty.

has tags => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    builder => "_build_tags",
);

sub _build_tags {
    my ($self) = @_;
    return $self->is_master->{tags} || {};
}

sub updated_since {
    my ( $self, $tv ) = @_;
    return tv_interval( $tv, $self->last_update_time ) > 0;
}

__PACKAGE__->meta->make_immutable;

1;

# vim: ts=4 sts=4 sw=4 et:
