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

use strict;
use warnings;
package MongoDB::Op::_ParallelScan;

# Encapsulate code path for parallelCollectionScan commands

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::Error;

use Types::Standard qw(
    HashRef
    Int
);

use namespace::clean;

has num_cursors => (
    is       => 'ro',
    required => 1,
    isa => Int,
);

has options => (
    is      => 'ro',
    required => 1,
    isa => HashRef,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_ReadOp
  MongoDB::Role::_MaybeMongoClient
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $session = delete $self->options->{session};

    my $command = [
        parallelCollectionScan => $self->coll_name,
        numCursors             => $self->num_cursors,
        ($link->accepts_wire_version(4) ?
            @{ $self->read_concern->as_args } : () ),
        %{$self->options},
    ];

    my $op = MongoDB::Op::_Command->_new(
        client          => $self->client,
        db_name         => $self->db_name,
        query           => $command,
        query_flags     => {},
        bson_codec      => $self->bson_codec,
        read_preference => $self->read_preference,
        ( defined $session ? ( session => $session ) : () )
    );

    return $op->execute( $link, $topology );
}

1;
