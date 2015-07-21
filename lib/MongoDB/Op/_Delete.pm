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

package MongoDB::Op::_Delete;

# Encapsulate a delete operation; returns a MongoDB::DeleteResult

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use Moo;

use MongoDB::BSON;
use MongoDB::DeleteResult;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use MongoDB::_Types -types;
use Types::Standard -types;
use Tie::IxHash;
use namespace::clean;

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

has filter => (
    is       => 'ro',
    required => 1,
    isa => Document,
);

has just_one => (
    is      => 'ro',
    default => 1,
    isa => Bool,
);

with qw/MongoDB::Role::_WriteOp/;

sub execute {
    my ( $self, $link ) = @_;

    my $res =
        $link->accepts_wire_version(2)
      ? $self->_command_delete($link)
      : $self->_legacy_op_delete($link);

    $res->assert;
    return $res;
}

sub _command_delete {
    my ( $self, $link, ) = @_;

    my $filter =
      ref( $self->filter ) eq 'ARRAY'
      ? { @{ $self->filter } }
      : $self->filter;

    my $op_doc = { q => $filter, limit => $self->just_one ? 1 : 0 };

    my $cmd = Tie::IxHash->new(
        delete       => $self->coll_name,
        deletes      => [ $op_doc ],
        writeConcern => $self->write_concern->as_struct,
    );

    return $self->_send_write_command( $link, $cmd, $op_doc, "MongoDB::DeleteResult" );
}

sub _legacy_op_delete {
    my ( $self, $link ) = @_;

    my $flags = { just_one => $self->just_one ? 1 : 0 };

    my $ns         = $self->db_name . "." . $self->coll_name;
    my $query_bson = $self->bson_codec->encode_one( $self->filter );
    my $op_bson    = MongoDB::_Protocol::write_delete( $ns, $query_bson, $flags );
    my $op_doc     = { q => $self->filter, limit => $flags->{just_one} };

    return $self->_send_legacy_op_with_gle( $link, $op_bson, $op_doc, "MongoDB::DeleteResult" );
}

sub _parse_cmd {
    my ( $self, $res ) = @_;
    return ( deleted_count => $res->{n} );
}

BEGIN {
    no warnings 'once';
    *_parse_gle = \&_parse_cmd;
}

1;
