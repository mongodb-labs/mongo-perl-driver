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
package MongoDB::Op::_Count;

# Encapsulate code path for count commands

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Op::_Command;
use Types::Standard qw(
    HashRef
    Maybe
);

use namespace::clean;

has filter => (
    is       => 'ro',
    required => 1,
    isa => Maybe[HashRef],
);

has options => (
    is       => 'ro',
    required => 1,
    isa => HashRef,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_ReadOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    if ( defined $self->options->{collation} and !$link->supports_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" );
    }

    my $command = [
        count => $self->coll_name,
        (defined $self->{filter} ? ( query => $self->{filter} ) : () ),
        ($link->supports_read_concern ?
            @{ $self->read_concern->as_args( $self->session ) } : () ),
        %{ $self->options },
    ];

    my $op = MongoDB::Op::_Command->_new(
        db_name             => $self->db_name,
        query               => $command,
        query_flags         => {},
        bson_codec          => $self->bson_codec,
        read_preference     => $self->read_preference,
        session             => $self->session,
        monitoring_callback => $self->monitoring_callback,
    );

    my $res = $op->execute( $link, $topology );
    return $res->output;
}

1;
