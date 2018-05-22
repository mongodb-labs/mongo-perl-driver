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
package MongoDB::Op::_KillCursors;

# Encapsulate a cursor kill operation; returns true

use version;
our $VERSION = 'v1.8.3';

use Moo;

use MongoDB::_Protocol;
use Types::Standard qw(
    ArrayRef
);

use namespace::clean;

has cursor_ids => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef,
);

with $_ for qw(
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_DatabaseOp
  MongoDB::Role::_PrivateConstructor
);

sub execute {
    my ( $self, $link ) = @_;

    if ( $link->accepts_wire_version(4) ) {
        # Spec says that failures should be ignored: cursor kills often happen
        # via destructors and users can't do anything about failure anyway.
        eval {
            MongoDB::Op::_Command->_new(
                db_name => $self->db_name,
                query   => [
                    killCursors => $self->coll_name,
                    cursors     => $self->cursor_ids,
                ],
                query_flags => {},
                bson_codec  => $self->bson_codec,
            )->execute($link);
        };
    }
    else {
        # Server never sends a reply, so ignoring failure here is automatic.
        $link->write( MongoDB::_Protocol::write_kill_cursors( @{ $self->cursor_ids } ) );
    }

    return 1;
}

1;
