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
package MongoDB::Op::_Update;

# Encapsulate an update operation; returns a MongoDB::UpdateResult

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::UpdateResult;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
    Boolish
    Document
);
use Types::Standard qw(
    Maybe
    ArrayRef
);
use Tie::IxHash;
use boolean;

use namespace::clean;

has filter => (
    is       => 'ro',
    required => 1,
    isa      => Document,
);

has update => (
    is       => 'ro',
    required => 1,
);

has is_replace => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has multi => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has upsert => (
    is       => 'ro',
);

has collation => (
    is       => 'ro',
    isa      => Maybe( [Document] ),
);

has arrayFilters => (
  is => 'ro',
  isa => Maybe( [ArrayRef[Document]] ),
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_SingleBatchDocWrite
  MongoDB::Role::_UpdatePreEncoder
  MongoDB::Role::_BypassValidation
);

# cached
my ($true, $false) = (true, false);

sub execute {
    my ( $self, $link ) = @_;

    if ( defined $self->collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" )
          if !$link->supports_collation;
        MongoDB::UsageError->throw(
            "Unacknowledged updates that specify a collation are not allowed")
          if ! $self->_should_use_acknowledged_write;
    }

    if ( defined $self->arrayFilters ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support arrayFilters" )
          if !$link->supports_arrayFilters;
        MongoDB::UsageError->throw(
            "Unacknowledged updates that specify arrayFilters are not allowed")
          if ! $self->_should_use_acknowledged_write;
    }

    my $orig_op = {
        q => (
            ref( $self->filter ) eq 'ARRAY'
            ? { @{ $self->filter } }
            : $self->filter
        ),
        u => $self->_pre_encode_update( $link->max_bson_object_size,
            $self->update, $self->is_replace ),
        multi  => $self->multi  ? $true : $false,
        upsert => $self->upsert ? $true : $false,
        ( defined $self->collation    ? ( collation    => $self->collation )    : () ),
        ( defined $self->arrayFilters ? ( arrayFilters => $self->arrayFilters ) : () ),
    };

    return $self->_send_legacy_op_noreply(
        $link,
        MongoDB::_Protocol::write_update(
            $self->full_name,
            $self->bson_codec->encode_one( $orig_op->{q}, { invalid_chars => '' } ),
            $self->_pre_encode_update( $link->max_bson_object_size,
                $orig_op->{u}, $self->is_replace )->{bson},
            {
                upsert => $orig_op->{upsert},
                multi  => $orig_op->{multi},
            },
        ),
        $orig_op,
        "MongoDB::UpdateResult",
        "update",
    ) if ! $self->_should_use_acknowledged_write;

    return $self->_send_write_command(
        $link,
        $self->_maybe_bypass(
            $link->supports_document_validation,
            [
                update  => $self->coll_name,
                updates => [
                    {
                        %$orig_op,
                        u => $self->_pre_encode_update(
                            $link->max_bson_object_size,
                            $orig_op->{u}, $self->is_replace
                        ),
                    }
                ],
                @{ $self->write_concern->as_args },
            ],
        ),
        $orig_op,
        "MongoDB::UpdateResult"
      )->assert
      if $link->supports_write_commands;

    return $self->_send_legacy_op_with_gle(
        $link,
        MongoDB::_Protocol::write_update(
            $self->full_name,
            $self->bson_codec->encode_one( $orig_op->{q}, { invalid_chars => '' } ),
            $self->_pre_encode_update( $link->max_bson_object_size,
                $orig_op->{u}, $self->is_replace )->{bson},
            {
                upsert => $orig_op->{upsert},
                multi  => $orig_op->{multi},
            },
        ),
        $orig_op,
        "MongoDB::UpdateResult",
        "update",
    )->assert;
}

sub _parse_cmd {
    my ( $self, $res ) = @_;

    return (
        matched_count  => ($res->{n} || 0)  - @{ $res->{upserted} || [] },
        modified_count => $res->{nModified},
        upserted_id    => $res->{upserted} ? $res->{upserted}[0]{_id} : undef,
    );
}

sub _parse_gle {
    my ( $self, $res, $orig_doc ) = @_;

    # For 2.4 and earlier, 'upserted' has _id only if the _id is an OID.
    # Otherwise, we have to pick it out of the update document or query
    # document when we see updateExisting is false but the number of docs
    # affected is 1

    my $upserted = $res->{upserted};
    if (! defined( $upserted )
        && exists( $res->{updatedExisting} )
        && !$res->{updatedExisting}
        && $res->{n} == 1 )
    {
        $upserted = $self->_find_id( $orig_doc->{u} );
        $upserted = $self->_find_id( $orig_doc->{q} ) unless defined $upserted;
    }

    return (
        matched_count  => ($upserted ? 0 : $res->{n} || 0),
        modified_count => undef,
        upserted_id    => $upserted,
    );
}

sub _find_id {
    my ($self, $doc) = @_;
    if (ref($doc) eq "BSON::Raw") {
       $doc = $self->bson_codec->decode_one($doc);
    }
    my $type = ref($doc);
    return (
          $type eq 'HASH' ? $doc->{_id}
        : $type eq 'ARRAY' ? do {
            my $i;
            for ( $i = 0; $i < @$doc; $i++ ) { last if $doc->[$i] eq '_id' }
            $i < $#$doc ? $doc->[ $i + 1 ] : undef;
          }
        : $type eq 'Tie::IxHash' ? $doc->FETCH('_id')
        : $doc->{_id} # hashlike?
    );
}

1;
