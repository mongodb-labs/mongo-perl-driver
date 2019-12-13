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
package MongoDB::Op::_Delete;

# Encapsulate a delete operation; returns a MongoDB::DeleteResult

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::DeleteResult;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
    Boolish
    Document
);
use Types::Standard qw(
    Maybe
);

use namespace::clean;

has filter => (
    is       => 'ro',
    required => 1,
    isa      => Document,
);

has just_one => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has collation => (
    is       => 'ro',
    isa      => Maybe( [Document] ),
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_SingleBatchDocWrite
);

sub execute {
    my ( $self, $link ) = @_;

    if ( defined $self->collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" )
          if !$link->supports_collation;
        MongoDB::UsageError->throw(
            "Unacknowledged deletes that specify a collation are not allowed")
          if ! $self->_should_use_acknowledged_write;
    }

    my $filter =
      ref( $self->filter ) eq 'ARRAY'
      ? { @{ $self->filter } }
      : $self->filter;

    my $op_doc = {
        q     => $filter,
        limit => $self->just_one ? 1 : 0,
        ( defined $self->collation ? ( collation => $self->collation ) : () ),
    };

    return (
        ! $self->_should_use_acknowledged_write
        ? (
            $self->_send_legacy_op_noreply(
                $link,
                MongoDB::_Protocol::write_delete(
                    $self->full_name,
                    $self->bson_codec->encode_one( $self->filter ),
                    { just_one => $self->just_one ? 1 : 0 }
                ),
                $op_doc,
                "MongoDB::DeleteResult",
                "delete",
            )
        )
        : $link->supports_write_commands
        ? (
            $self->_send_write_command(
                $link,
                [
                    delete       => $self->coll_name,
                    deletes      => [$op_doc],
                    @{ $self->write_concern->as_args },
                ],
                $op_doc,
                "MongoDB::DeleteResult"
            )->assert
          )
        : (
            $self->_send_legacy_op_with_gle(
                $link,
                MongoDB::_Protocol::write_delete(
                    $self->full_name,
                    $self->bson_codec->encode_one( $self->filter ),
                    { just_one => $self->just_one ? 1 : 0 }
                ),
                $op_doc,
                "MongoDB::DeleteResult",
                "delete",
            )->assert
        )
    );
}

sub _parse_cmd {
    my ( $self, $res ) = @_;
    return ( deleted_count => $res->{n} || 0 );
}

BEGIN {
    no warnings 'once';
    *_parse_gle = \&_parse_cmd;
}

1;
