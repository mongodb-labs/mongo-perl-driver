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
package MongoDB::Op::_FSyncUnlock;

# Encapsulate collection list operations; returns arrayref of collection
# names

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::Op::_Query;
use MongoDB::ReadConcern;
use MongoDB::ReadPreference;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    InstanceOf
);
use Tie::IxHash;

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->supports_fsync_command
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
        db_name             => $self->db_name,
        query               => $cmd,
        query_flags         => {},
        read_preference     => MongoDB::ReadPreference->new,
        bson_codec          => $self->bson_codec,
        monitoring_callback => $self->monitoring_callback,
    );

    my $res = $op->execute( $link, $topology );

    return $res->{output};
}

sub _legacy_fsync_unlock {
    my ( $self, $link, $topology ) = @_;

    my $op = MongoDB::Op::_Query->_new(
        bson_codec      => $self->bson_codec,
        client          => $self->client,
        coll_name       => '$cmd.sys.unlock',
        db_name         => 'admin',
        filter          => {},
        full_name       => 'admin.$cmd.sys.unlock',
        options         => MongoDB::Op::_Query->precondition_options( { limit => -1 } ),
        read_concern    => MongoDB::ReadConcern->new,
        read_preference => MongoDB::ReadPreference->new,
        monitoring_callback => $self->monitoring_callback,
    );

    return $op->execute( $link, $topology )->next;
}

1;
