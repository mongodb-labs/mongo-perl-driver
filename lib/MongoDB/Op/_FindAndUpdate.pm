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

package MongoDB::Op::_FindAndUpdate;

# Encapsulate find_and_update operation; atomically update and return doc

use version;
our $VERSION = 'v1.1.1';

use Moo;

use boolean;
use MongoDB::Error;
use MongoDB::Op::_Command;
use Types::Standard qw(
    InstanceOf
    Str
    HashRef
    Maybe
);

use MongoDB::_Types qw(
    WriteConcern
);

use Try::Tiny;
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

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

has filter => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

has modifier => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

has options => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

has write_concern => (
    is       => 'ro',
    required => 1,
    isa      => WriteConcern,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_ReadOp
  MongoDB::Role::_CommandCursorOp
  MongoDB::Role::_BypassValidation
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my ( undef, $command ) = $self->_maybe_bypass(
        $link,
        [
            findAndModify => $self->coll_name,
            query         => $self->filter,
            update        => $self->modifier,
            (
                $link->accepts_wire_version(4)
                ? ( @{ $self->write_concern->as_args } )
                : ()
            ),
            %{ $self->options },
        ]
    );

    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => $command,
        query_flags     => {},
        bson_codec      => $self->bson_codec,
    );

    my $result;
    try {
        $result = $op->execute( $link, $topology );
        $result = $result->{output};
    }
    catch {
        die $_ unless $_ eq 'No matching object found';
    };

    return $result->{value} if $result;
    return;
}

1;
