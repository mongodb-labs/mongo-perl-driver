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
package MongoDB::Op::_Distinct;

# Encapsulate distinct operation; return MongoDB::QueryResult

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    InstanceOf
    HashRef
    Str
);

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf ['MongoDB::MongoClient'],
);

has fieldname=> (
    is       => 'ro',
    required => 1,
    isa => Str,
);

has filter => (
    is      => 'ro',
    required => 1,
    isa => Document,
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
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $options = $self->options;

    if ( defined $options->{collation} and !$link->supports_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" );
    }

    my $filter =
      ref( $self->filter ) eq 'ARRAY'
      ? { @{ $self->filter } }
      : $self->filter;

    my @command = (
        distinct => $self->coll_name,
        key      => $self->fieldname,
        query    => $filter,
        ($link->supports_read_concern ?
            @{ $self->read_concern->as_args( $self->session) } : ()),
        %$options
    );

    my $op = MongoDB::Op::_Command->_new(
        db_name             => $self->db_name,
        query               => Tie::IxHash->new(@command),
        query_flags         => {},
        read_preference     => $self->read_preference,
        bson_codec          => $self->bson_codec,
        session             => $self->session,
        monitoring_callback => $self->monitoring_callback,
    );

    my $res = $op->execute( $link, $topology );

    $res->output->{cursor} = {
        ns         => '',
        id         => 0,
        firstBatch => ( delete $res->output->{values} ) || [],
    };

    return $self->_build_result_from_cursor($res);
}

1;
