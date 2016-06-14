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

package MongoDB::Op::_FindAndDelete;

# Encapsulate find_and_delete operation; atomically delete and return doc

use version;
our $VERSION = 'v1.4.3';

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
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $command = [
        findAndModify   => $self->coll_name,
        query           => $self->filter,
        remove          => true,
        ($link->accepts_wire_version(4) ?
            (@{ $self->write_concern->as_args })
            : () ),
        %{ $self->options },
    ];

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

    # findAndModify returns ok:1 even for write concern errors, so 
    # we must check and throw explicitly
    if ( $result->{writeConcernError} ) {
        MongoDB::WriteConcernError->throw(
            message => $result->{writeConcernError}{errmsg},
            result  => $result,
            code    => WRITE_CONCERN_ERROR,
        );
    }

    return $result->{value} if $result;
    return;
}

1;
