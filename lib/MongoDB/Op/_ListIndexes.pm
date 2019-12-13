#  Copyright 2014 - present MongoDB, Inc.
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

package MongoDB::Op::_ListIndexes;

# Encapsulate index list operation; returns array ref of index documents

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Error;
use MongoDB::Op::_Command;
use MongoDB::Op::_Query;
use MongoDB::ReadConcern;
use MongoDB::ReadPreference;
use Types::Standard qw(
  InstanceOf
);
use Tie::IxHash;
use Safe::Isa;
use MongoDB::_Types qw(
    ReadPreference
);

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

has read_preference => (
    is  => 'rw', # rw for Op::_Query which can be modified by Cursor
    required => 1,
    isa => ReadPreference,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->supports_list_commands
      ? $self->_command_list_indexes( $link, $topology )
      : $self->_legacy_list_indexes( $link, $topology );

    return $res;
}

sub _command_list_indexes {
    my ( $self, $link, $topology ) = @_;

    my $op = MongoDB::Op::_Command->_new(
        db_name     => $self->db_name,
        query       => Tie::IxHash->new( listIndexes => $self->coll_name, cursor => {} ),
        query_flags => {},
        bson_codec  => $self->bson_codec,
        monitoring_callback => $self->monitoring_callback,
        read_preference     => $self->read_preference,
        session     => $self->session,
    );

    my $res = eval {
        $op->execute( $link, $topology );
    } or do {
        my $error = $@ || "Unknown error";
        unless ( $error->$_isa("MongoDB::DatabaseError") and $error->code == NAMESPACE_NOT_FOUND()) {
            die $error;
        }
        undef;
    };

    return $res
      ? $self->_build_result_from_cursor($res)
      : $self->_empty_query_result($link);
}

sub _legacy_list_indexes {
    my ( $self, $link, $topology ) = @_;

    my $ns = $self->db_name . "." . $self->coll_name;
    my $op = MongoDB::Op::_Query->_new(
        filter              => Tie::IxHash->new( ns => $ns ),
        options             => MongoDB::Op::_Query->precondition_options({}),
        bson_codec          => $self->bson_codec,
        client              => $self->client,
        coll_name           => 'system.indexes',
        db_name             => $self->db_name,
        full_name           => $self->db_name . '.system.indexes',
        read_concern        => MongoDB::ReadConcern->new,
        read_preference     => $self->read_preference || MongoDB::ReadPreference->new,
        monitoring_callback => $self->monitoring_callback,
    );

    return $op->execute( $link, $topology );
}

1;
