#  Copyright 2015 - present MongoDB, Inc.
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

use strict;
use warnings;
package MongoDB::Op::_Explain;

# Encapsulate code path for explain commands/queries

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Op::_Command;
use Types::Standard qw(
    InstanceOf
);
use Tie::IxHash;
use boolean;

use namespace::clean;

has query => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::Op::_Query'],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_ReadOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->supports_explain_command
      ? $self->_command_explain( $link, $topology )
      : $self->_legacy_explain( $link, $topology );

    return $res;
}

sub _command_explain {
    my ( $self, $link, $topology ) = @_;

    my $cmd = Tie::IxHash->new( @{ $self->query->_as_command } );

    # XXX need to standardize error here
    if ($self->query->has_hint) {
        # cannot use hint on explain, throw error
        MongoDB::Error->throw(
            message => "cannot use 'hint' with 'explain'",
        );
    }

    my $op = MongoDB::Op::_Command->_new(
        db_name => $self->db_name,
        query   => [
            explain => $cmd,
            @{ $self->read_concern->as_args( $self->session ) }
        ],
        query_flags         => {},
        read_preference     => $self->read_preference,
        bson_codec          => $self->bson_codec,
        session             => $self->session,
        monitoring_callback => $self->monitoring_callback,
    );
    my $res = $op->execute( $link, $topology );

    return $res->{output};
}

sub _legacy_explain {
    my ( $self, $link, $topology ) = @_;

    my $orig_query  = $self->query;
    my $cloned_opts = { %{ $orig_query->options } };
    $cloned_opts->{explain} = 1;

    # per David Storch, drivers *must* send a negative limit to instruct
    # the query planner analysis module to add a LIMIT stage.  For older
    # explain implementations, it also ensures a cursor isn't left open.
    $cloned_opts->{limit} = ( -1 * abs( $cloned_opts->{limit} ) );

    my $new_query = MongoDB::Op::_Query->_new(
        %$orig_query,
        # override options from original
        options => $cloned_opts
    );

    return $new_query->execute( $link, $topology )->next;
}

1;
