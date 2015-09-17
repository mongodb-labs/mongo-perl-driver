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

package MongoDB::Op::_GetMore;

# Encapsulate a cursor fetch operation; returns raw results object
# (after inflation from BSON)

use version;
our $VERSION = 'v1.1.0';

use Moo;

use MongoDB::_Constants;
use Types::Standard qw(
    Maybe
    Any
    InstanceOf
    Num
    Str
);
use MongoDB::_Protocol;
use Tie::IxHash;
use Math::BigInt;

use Devel::StackTrace;

use namespace::clean;

has ns => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has db_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has coll_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

has cursor_id => (
    is       => 'ro',
    required => 1,
    isa      => Any,
);

has batch_size => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

has max_time_ms => (
    is       => 'ro',
    isa      => Maybe[Num],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseOp
  MongoDB::Role::_CommandOp
);

sub execute {
    my ( $self, $link ) = @_;

    my $res =
        $link->accepts_wire_version(4)
      ? $self->_command_get_more( $link )
      : $self->_legacy_get_more( $link );

    return $res;
}

sub _command_get_more {
    my ( $self, $link ) = @_;

    my ( $db_name, $coll_name ) = split(/\./, $self->ns, 2);

    my $cmd = Tie::IxHash->new(
        getMore         => $self->cursor_id,
        collection      => $self->coll_name,
        $self->batch_size > 0 ? (batchSize => $self->batch_size) : (),
        defined $self->max_time_ms ? (maxTimeMS => $self->max_time_ms) : (),
    );

    my $res = $self->_send_command( $link, $cmd );
    my $c = $res->{cursor};
    my $batch = $c->{nextBatch};

    return {
        cursor_id       => $c->{id},
        flags           => {},
        starting_from   => 0,
        number_returned => scalar @$batch,
        docs            => $batch,
    };
}

sub _legacy_get_more {
    my ( $self, $link ) = @_;

    my ( $op_bson, $request_id ) = MongoDB::_Protocol::write_get_more( map { $self->$_ }
          qw/ns cursor_id batch_size/ );

    my $result =
      $self->_query_and_receive( $link, $op_bson, $request_id, $self->bson_codec );

    $result->{address} = $link->address;

    return $result;
}

1;
