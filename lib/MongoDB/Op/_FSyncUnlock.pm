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

package MongoDB::Op::_FSyncUnlock;

# Encapsulate collection list operations; returns arrayref of collection
# names

use version;
our $VERSION = 'v0.999.999.7';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::Op::_Query;
use MongoDB::QueryResult::Filtered;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    HashRef
    InstanceOf
    Str
);
use Tie::IxHash;
use namespace::clean;

has db_name => (
    is       => 'ro',
    required => 1,
    isa => Str,
);

has client => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_ReadOp
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->accepts_wire_version(4)
      ? $self->_command_fsync_unlock( $link, $topology )
      : $self->_legacy_fsync_unlock( $link, $topology );

    return $res;
}

sub _command_fsync_unlock {
    my ( $self, $link, $topology ) = @_;

    my $cmd = Tie::IxHash->new(
        fsyncUnlock     => 1,
    );

    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => $cmd,
        query_flags     => {},
        read_preference => $self->read_preference,
        bson_codec      => $self->bson_codec,
    );

    my $res = $op->execute( $link, $topology );

    return $res->{output};
}

sub _legacy_fsync_unlock {
    my ( $self, $link, $topology ) = @_;
    
    my $query = MongoDB::_Query->_new(
        modifiers           => {},
        allowPartialResults => 0,
        batchSize           => 0,
        comment             => '',
        cursorType          => 'non_tailable',
        maxTimeMS           => 0,
        noCursorTimeout     => 0,
        oplogReplay         => 0,
        projection          => undef,
        skip                => 0,
        sort                => undef,
        db_name             => 'admin',
        coll_name           => '$cmd.sys.unlock',
        limit               => -1,
        bson_codec          => $self->bson_codec,
        client              => $self->client,
        read_preference     => $self->read_preference,
    ); 
    
    my $op = $query->as_query_op();

    return $op->execute( $link, $topology )->next;
}

1;
