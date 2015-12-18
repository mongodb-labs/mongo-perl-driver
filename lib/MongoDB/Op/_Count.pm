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

package MongoDB::Op::_Count;

# Encapsulate code path for count commands

use version;
our $VERSION = 'v1.2.2';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::Error;

use MongoDB::_Types qw(
    Document
);

use Types::Standard qw(
    Str
    InstanceOf
    HashRef
);

use Tie::IxHash;
use boolean;
use namespace::clean;

has filter => (
    is       => 'ro',
    required => 1,
    isa => HashRef,
);

has options => (
    is       => 'ro',
    required => 1,
    isa => HashRef,
);

has db_name => (
    is       => 'ro',
    required => 1,
    isa => Str,
);

has coll_name => (
    is       => 'ro',
    required => 1,
    isa => Str,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_ReadOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $command = [
        count => $self->coll_name,
        query => $self->filter,

        ($link->accepts_wire_version(4) ?
            @{ $self->read_concern->as_args } : () ),

        %{ $self->options },
    ];

    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => $command,
        query_flags     => {},
        bson_codec      => $self->bson_codec,
        read_preference => $self->read_preference,
    );

    my $res = $op->execute( $link, $topology );
    return $res->output;
}

1;
