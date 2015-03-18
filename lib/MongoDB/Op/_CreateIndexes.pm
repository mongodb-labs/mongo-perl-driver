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
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;

use MongoDB::CommandResult;
use MongoDB::_Types -types;
use Types::Standard -types;
use MongoDB::Op::_BatchInsert;
use MongoDB::_Types;
use Tie::IxHash;
use namespace::clean -except => 'meta';

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has coll_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has indexes => (
    is       => 'ro',
    isa      => ArrayRef[HashRef], # XXX ArrayRef[IndexModel]?
    required => 1,
);

has write_concern => (
    is       => 'ro',
    isa      => WriteConcern,
    required => 1,
);

with qw/MongoDB::Role::_CommandOp/;

sub execute {
    my ( $self, $link ) = @_;

    my $res =
        $link->accepts_wire_version(2)
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

    return MongoDB::CommandResult->new(
        result => $self->write_concern->is_safe ? $res : { ok => 1 },
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

    my $op = MongoDB::Op::_BatchInsert->new(
        db_name       => $self->db_name,
        coll_name     => "system.indexes",
        documents     => $indexes,
        write_concern => $self->write_concern,
        check_keys    => 0,
    );

    return $op->execute($link);
}

1;
