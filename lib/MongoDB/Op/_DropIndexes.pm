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
package MongoDB::Op::_DropIndexes;

# Implements index drops; returns a MongoDB::CommandResult

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::Error;
use MongoDB::Op::_Command;
use Safe::Isa;
use Types::Standard qw(
  Str
);

use namespace::clean;

has index_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

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
            dropIndexes => $self->coll_name,
            index       => $self->index_name,
            ( $link->accepts_wire_version(5) ? ( @{ $self->write_concern->as_args } ) : () ),
        ],
        query_flags         => {},
        bson_codec          => $self->bson_codec,
        monitoring_callback => $self->monitoring_callback,
    );

    my $res;
    eval {
        $res = $op->execute($link);
        $res->assert_no_write_concern_error;
    };
    # XXX This logic will be a problem for command monitoring - may need to
    # move into Op::_Command as an 'error_filter' callback or something.
    if ( my $err = $@ ) {
        if ( $err->$_isa("MongoDB::DatabaseError") ) {
            # 2.6 and 3.0 don't have the code, so we fallback to string
            # matching on the error message
            return $err->result
              if $err->code == INDEX_NOT_FOUND() || $err->message =~ /index not found with name/;
        }
        die $err;
    }

    return $res;
}

1;
