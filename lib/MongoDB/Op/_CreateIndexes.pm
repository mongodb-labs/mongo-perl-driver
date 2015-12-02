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

package MongoDB::Op::_CreateIndexes;

# Encapsulate index creation operations; returns a MongoDB::CommandResult
# or a MongoDB::InsertManyResult, depending on the server version

use version;
our $VERSION = 'v1.1.2';

use Moo;

use MongoDB::CommandResult;
use MongoDB::_Constants;
use MongoDB::_Types -types;
use MongoDB::Op::_BatchInsert;
use Types::Standard qw(
    ArrayRef
    HashRef
    Str
);
use MongoDB::_Types qw(
    WriteConcern
);
use Tie::IxHash;
use namespace::clean;

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

has indexes => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef [HashRef],
);

has write_concern => (
    is       => 'ro',
    required => 1,
    isa      => WriteConcern,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CommandOp
);

sub execute {
    my ( $self, $link ) = @_;

    my $res =
        $link->does_write_commands
      ? $self->_command_create_indexes($link)
      : $self->_legacy_index_insert($link);

    $res->assert;

    return $res;
}

sub _command_create_indexes {
    my ( $self, $link, $op_doc ) = @_;

    my $cmd = Tie::IxHash->new(
        createIndexes => $self->coll_name,
        indexes       => $self->indexes,
    );

    my $res = $self->_send_command( $link, $cmd );

    return MongoDB::CommandResult->_new(
        output => $self->write_concern->is_acknowledged ? $res : { ok => 1 },
        address => $link->address,
    );
}

sub _legacy_index_insert {
    my ( $self, $link, $op_doc ) = @_;

    # construct docs for an insert many op
    my $ns = join( ".", $self->db_name, $self->coll_name );
    my $indexes = [
        map {
            { %$_, ns => $ns }
        } @{ $self->indexes }
    ];

    my $op = MongoDB::Op::_BatchInsert->_new(
        db_name       => $self->db_name,
        coll_name     => "system.indexes",
        documents     => $indexes,
        write_concern => $self->write_concern,
        bson_codec    => $self->bson_codec,
        check_keys    => 0,
        ordered       => 1,
    );

    return $op->execute($link);
}

1;
