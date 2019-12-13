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

package MongoDB::Op::_GetMore;

# Encapsulate a cursor fetch operation; returns raw results object
# (after inflation from BSON)

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::_Protocol;
use Types::Standard qw(
  Maybe
  Any
);
use MongoDB::_Types qw(
    Numish
);

use namespace::clean;

has cursor_id => (
    is       => 'ro',
    required => 1,
    isa      => Any,
);

has batch_size => (
    is       => 'ro',
    required => 1,
    isa      => Numish,
);

has max_time_ms => (
    is  => 'ro',
    isa => Numish,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_OpReplyParser
  MongoDB::Role::_DatabaseOp
);

sub execute {
    my ( $self, $link ) = @_;

    my $res =
        $link->supports_query_commands
      ? $self->_command_get_more($link)
      : $self->_legacy_get_more($link);

    return $res;
}

sub _command_get_more {
    my ( $self, $link ) = @_;

    my $op = MongoDB::Op::_Command->_new(
        db_name             => $self->db_name,
        query               => $self->_as_command,
        query_flags         => {},
        bson_codec          => $self->bson_codec,
        session             => $self->session,
        monitoring_callback => $self->monitoring_callback,
    );

    my $c = $op->execute($link)->output->{cursor};
    my $batch = $c->{nextBatch} || [];

    return {
        cursor_id => $c->{id} || 0,
        flags => {},
        starting_from   => 0,
        number_returned => scalar @$batch,
        docs            => $batch,
    };
}

sub _as_command {
    my ($self) = @_;
    return [
        getMore    => $self->cursor_id,
        collection => $self->coll_name,
        ( $self->batch_size > 0 ? ( batchSize => $self->batch_size )  : () ),
        ( $self->max_time_ms    ? ( maxTimeMS => $self->max_time_ms ) : () ),
    ];
}

sub _legacy_get_more {
    my ( $self, $link ) = @_;

    my ( $op_bson, $request_id ) = MongoDB::_Protocol::write_get_more( map { $self->$_ }
          qw/full_name cursor_id batch_size/ );

    my $result =
      $self->_query_and_receive( $link, $op_bson, $request_id, $self->bson_codec );

    $result->{address} = $link->address;

    return $result;
}

1;
