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

package MongoDB::Op::_Explain;

# Encapsulate code path for explain commands/queries 

use version;
our $VERSION = 'v0.999.999.7';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::Op::_Query;
use MongoDB::QueryResult::Filtered;
use MongoDB::_Constants;
use MongoDB::Error;
use MongoDB::CommandResult;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    HashRef
    InstanceOf
    Str
);
use Tie::IxHash;
use boolean;
use namespace::clean;

has query => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::_Query'],
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

    my $res =
        $link->accepts_wire_version(3)
      ? $self->_command_explain( $link, $topology )
      : $self->_legacy_explain( $link, $topology );

    return $res;
}

sub _command_explain {
    my ( $self, $link, $topology ) = @_;

    my $cmd = $self->query->as_query_op->as_command;
    
    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => {
            explain   => $cmd,
            #verbosity => XXX Unimplemented,
        },
        query_flags     => {},
        read_preference => $self->read_preference,
        bson_codec      => $self->bson_codec,
    );
    my $res = $op->execute( $link, $topology );

    # XXX need to standardize error here
    if (defined $self->query->modifiers->{hint}) {
        # cannot use hint on explain, throw error
        MongoDB::DatabaseError->throw(
            message => "cannot use 'hint' with 'explain",
            result => MongoDB::CommandResult->_new(
                output => $res,
                address => $link->address,
            ),
        );
    }
    return $res->{output};
}

sub _legacy_explain {
    my ( $self, $link, $topology ) = @_;

    my $new_query = $self->query->clone;
    $new_query->modifiers->{'$explain'} = true;

    # per David Storch, drivers *must* send a negative limit to instruct
    # the query planner analysis module to add a LIMIT stage.  For older
    # explain implementations, it also ensures a cursor isn't left open.
    $new_query->limit( -1 * abs( $new_query->limit ) );

    return $new_query->execute->next;
}

1;
