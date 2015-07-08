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

package MongoDB::Op::_InsertOne;

# Encapsulate a single-document insert operation; returns a
# MongoDB::InsertOneResult

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use Moose;

use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::InsertOneResult;
use MongoDB::OID;
use MongoDB::_Protocol;
use MongoDB::_Types -types;
use Types::Standard -types;
use Tie::IxHash;
use boolean;
use namespace::clean -except => 'meta';

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has coll_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has document => (
    is       => 'ro',
    isa      => IxHash,
    coerce   => 1,
    required => 1,
);

has _doc_id => (
    is        => 'ro',
    isa       => Any,
    writer    => '_set_doc_id',
);

with qw/MongoDB::Role::_WriteOp/;

sub execute {
    my ( $self, $link ) = @_;

    my $document = $self->document;

    my $id = $self->_set_doc_id(
        $document->EXISTS('_id') ? $document->FETCH('_id') : MongoDB::OID->new
    );

    # XXX until we have a proper BSON::Raw class, we bless on the fly
    my $bson_doc = $self->bson_codec->encode_one(
        $document,
        {
            invalid_chars => '.',
            max_length    => $link->max_bson_object_size,
            first_key => '_id',
            first_value => $id,
        }
    );

    my $insert_doc = bless \$bson_doc, "MongoDB::BSON::Raw";

    my $res =
        $link->accepts_wire_version(2)
      ? $self->_command_insert( $link, $insert_doc )
      : $self->_legacy_op_insert( $link, $insert_doc );

    $res->assert;
    return $res;
}

sub _command_insert {
    my ( $self, $link, $insert_doc ) = @_;

    my $cmd = Tie::IxHash->new(
        insert       => $self->coll_name,
        documents    => [$insert_doc],
        writeConcern => $self->write_concern->as_struct,
    );

    return $self->_send_write_command( $link, $cmd, $self->document, "MongoDB::InsertOneResult" );
}

sub _legacy_op_insert {
    my ( $self, $link, $insert_doc ) = @_;

    my $ns = $self->db_name . "." . $self->coll_name;
    my $op_bson = MongoDB::_Protocol::write_insert( $ns, $$insert_doc );

    return $self->_send_legacy_op_with_gle( $link, $op_bson, $self->document,
        "MongoDB::InsertOneResult" );
}

sub _parse_cmd {
    my ( $self, $res ) = @_;
    return ( $res->{ok} ? ( inserted_id => $self->_doc_id ) : () );
}

BEGIN {
    no warnings 'once';
    *_parse_gle = \&_parse_cmd;
}

__PACKAGE__->meta->make_immutable;

1;
