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
package MongoDB::Op::_Update;

# Encapsulate an update operation; returns a MongoDB::UpdateResult

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::UpdateResult;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    Bool
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
    isa      => Bool,
);

has multi => (
    is       => 'ro',
    required => 1,
    isa      => Bool,
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
          if !$self->write_concern->is_acknowledged;
    }

    my $orig_op = {
        q => (
            ref( $self->filter ) eq 'ARRAY'
            ? { @{ $self->filter } }
            : $self->filter
        ),
        u      => $self->update,
        multi  => $self->multi ? $true : $false,
        upsert => $self->upsert ? $true : $false,
        ( defined $self->collation ? ( collation => $self->collation ) : () ),
        ( defined $self->arrayFilters ? ( arrayFilters => $self->arrayFilters ) : () ),
    };

    return ! $self->write_concern->is_acknowledged
      ? (
        $self->_send_legacy_op_noreply(
            $link,
            MongoDB::_Protocol::write_update(
                $self->full_name,
                $self->bson_codec->encode_one( $orig_op->{q}, { invalid_chars => '' } ),
                $self->_pre_encode_update( $link, $orig_op->{u}, $self->is_replace )->{bson},
                {
                    upsert => $orig_op->{upsert},
                    multi  => $orig_op->{multi},
                },
            ),
            $orig_op,
            "MongoDB::UpdateResult"
        )
      )
      : $link->does_write_commands
      ? (
        $self->_send_write_command(
            $self->_maybe_bypass(
                $link,
                [
                    update  => $self->coll_name,
                    updates => [
                        {
                            %$orig_op, u => $self->_pre_encode_update( $link, $orig_op->{u}, $self->is_replace ),
                        }
                    ],
                    @{ $self->write_concern->as_args },
                ],
            ),
            $orig_op,
            "MongoDB::UpdateResult"
        )->assert
      )
      : (
        $self->_send_legacy_op_with_gle(
            $link,
            MongoDB::_Protocol::write_update(
                $self->full_name,
                $self->bson_codec->encode_one( $orig_op->{q}, { invalid_chars => '' } ),
                $self->_pre_encode_update( $link, $orig_op->{u}, $self->is_replace )->{bson},
                {
                    upsert => $orig_op->{upsert},
                    multi  => $orig_op->{multi},
                },
            ),
            $orig_op,
            "MongoDB::UpdateResult"
        )->assert
      );
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
        $upserted = _find_id( $orig_doc->{u} );
        $upserted = _find_id( $orig_doc->{q} ) unless defined $upserted;
    }

    return (
        matched_count  => ($upserted ? 0 : $res->{n} || 0),
        modified_count => undef,
        upserted_id    => $upserted,
    );
}

sub _find_id {
    my ($doc) = @_;
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
