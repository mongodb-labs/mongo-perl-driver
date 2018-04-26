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
package MongoDB::Op::_ListIndexes;

# Encapsulate index list operation; returns array ref of index documents

use version;
our $VERSION = 'v1.999.0';

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
use Try::Tiny;
use Safe::Isa;

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->accepts_wire_version(3)
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
    );

    my $res = try {
        $op->execute( $link, $topology );
    }
    catch {
        if ( $_->$_isa("MongoDB::DatabaseError") ) {
            return undef if $_->code == NAMESPACE_NOT_FOUND(); ## no critic: make $res undef
        }
        die $_;
    };

    return $res ? $self->_build_result_from_cursor($res) : $self->_empty_query_result($link);
}

sub _legacy_list_indexes {
    my ( $self, $link, $topology ) = @_;

    my $ns = $self->db_name . "." . $self->coll_name;
    my $op = MongoDB::Op::_Query->_new(
        modifiers           => {},
        allowPartialResults => 0,
        batchSize           => 0,
        comment             => '',
        cursorType          => 'non_tailable',
        limit               => 0,
        maxAwaitTimeMS      => 0,
        maxTimeMS           => 0,
        noCursorTimeout     => 0,
        oplogReplay         => 0,
        projection          => undef,
        skip                => 0,
        sort                => undef,
        db_name             => $self->db_name,
        coll_name           => 'system.indexes',
        full_name           => $self->db_name . '.system.indexes',
        bson_codec          => $self->bson_codec,
        client              => $self->client,
        read_preference     => MongoDB::ReadPreference->new,
        read_concern        => MongoDB::ReadConcern->new,
        filter              => Tie::IxHash->new( ns => $ns ),
        monitoring_callback => $self->monitoring_callback,
    );
  
    return $op->execute( $link, $topology );
}

1;
