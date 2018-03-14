#
#  Copyright 2016 MongoDB, Inc.
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
package MongoDB::Op::_DropCollection;

# Implements a collection drop; returns a MongoDB::CommandResult

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::Error;
use MongoDB::Op::_Command;
use Safe::Isa;
use Try::Tiny;
use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_WriteOp
);

sub execute {
    my ( $self, $link ) = @_;

    my $op = MongoDB::Op::_Command->_new(
        db_name => $self->db_name,
        query   => [
            drop => $self->coll_name,
            ( $link->accepts_wire_version(5) ? ( @{ $self->write_concern->as_args } ) : () ),
        ],
        query_flags => {},
        bson_codec  => $self->bson_codec,
        session     => $self->session,
    );

    my $res;
    try {
        $res = $op->execute($link);
        $res->assert_no_write_concern_error;
    }
    catch {
        if ( $_->$_isa("MongoDB::DatabaseError") ) {
            return undef if $_->code == NAMESPACE_NOT_FOUND() || $_->message =~ /^ns not found/; ## no critic: make $res undef
        }
        die $_;
    };

    return $res;
}

1;
