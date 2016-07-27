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
our $VERSION = 'v1.4.5';

use Moo;

use MongoDB::CommandResult;
use MongoDB::_Constants;
use MongoDB::_Types -types;
use MongoDB::Op::_Command;
use MongoDB::Op::_BatchInsert;
use Types::Standard qw(
    ArrayRef
    HashRef
    Str
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

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_WriteOp
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

    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => [ createIndexes => $self->coll_name, indexes => $self->indexes, ],
        query_flags     => {},
        bson_codec      => $self->bson_codec,
    );

    return $op->execute( $link );
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
        full_name     => (join ".", $self->db_name, "system.indexes"),
        documents     => $indexes,
        write_concern => $self->write_concern,
        bson_codec    => $self->bson_codec,
        check_keys    => 0,
        ordered       => 1,
    );

    return $op->execute($link);
}

1;
