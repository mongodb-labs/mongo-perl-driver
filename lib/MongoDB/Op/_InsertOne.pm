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
package MongoDB::Op::_InsertOne;

# Encapsulate a single-document insert operation; returns a
# MongoDB::InsertOneResult

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::Error;
use MongoDB::InsertOneResult;
use MongoDB::_Protocol;

use namespace::clean;

has document => (
    is       => 'ro',
    required => 1,
);

# this starts undef and gets initialized during processing
has _doc_id => (
    is       => 'ro',
    init_arg => undef,
    writer   => '_set_doc_id',
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_SingleBatchDocWrite
  MongoDB::Role::_InsertPreEncoder
  MongoDB::Role::_BypassValidation
);

sub execute {
    my ( $self,     $link )       = @_;
    my ( $orig_doc, $insert_doc ) = ( $self->document );

    ( $insert_doc = $self->_pre_encode_insert( $link, $orig_doc, '.' ) ),
      ( $self->_set_doc_id( $insert_doc->{metadata}{_id} ) );

    return ! $self->write_concern->is_acknowledged
      ? (
        $self->_send_legacy_op_noreply( $link,
            MongoDB::_Protocol::write_insert( $self->full_name, $insert_doc->{bson} ),
            $insert_doc, "MongoDB::UnacknowledgedResult", "insert" )
      )
      : $link->does_write_commands
      ? (
        $self->_send_write_command(
            $self->_maybe_bypass(
                $link,
                [
                    insert    => $self->coll_name,
                    documents => [$insert_doc],
                    @{ $self->write_concern->as_args },
                ],
            ),
            $orig_doc,
            "MongoDB::InsertOneResult",
        )->assert
      )
      : (
        $self->_send_legacy_op_with_gle( $link,
            MongoDB::_Protocol::write_insert( $self->full_name, $insert_doc->{bson} ),
            $insert_doc, "MongoDB::InsertOneResult", "insert" )->assert
      );
}

sub _parse_cmd {
    my ( $self, $res ) = @_;
    return ( $res->{ok} ? ( inserted_id => $self->_doc_id ) : () );
}

BEGIN {
    no warnings 'once';
    *_parse_gle = \&_parse_cmd;
}

1;
