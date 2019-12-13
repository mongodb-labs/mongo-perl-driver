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
package MongoDB::Role::_InsertPreEncoder;

# MongoDB interface for pre-encoding and validating docs to insert

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use BSON::Raw;
use BSON::OID;

use namespace::clean;

requires qw/bson_codec/;

# takes MongoDB::_Link and ref of type Document; returns
# blessed BSON encode doc and the original/generated _id
sub _pre_encode_insert {
    my ( $self, $max_bson_size, $doc, $invalid_chars ) = @_;

    my $type = ref($doc);

    my $id = (
          $type eq 'HASH' ? $doc->{_id}
        : $type eq 'ARRAY' || $type eq 'BSON::Doc' ? do {
            my $i;
            for ( $i = 0; $i < @$doc; $i++ ) { last if $doc->[$i] eq '_id' }
            $i < $#$doc ? $doc->[ $i + 1 ] : undef;
          }
        : $type eq 'Tie::IxHash' ? $doc->FETCH('_id')
        : $type eq 'BSON::Raw' ? do {
            my $decoded_doc = $self->bson_codec->decode_one(
                $doc->bson,
                { ordered => 1 }
            );
            $decoded_doc->{_id};
            }
        : $doc->{_id} # hashlike?
    );
    if ( ! defined $id ) {
        my $creator = $self->bson_codec->can("create_oid");
        $id = $creator ? $creator->() : BSON::OID->new();
    }
    my $bson_doc = $self->bson_codec->encode_one(
        $doc,
        {
            invalid_chars => $invalid_chars,
            max_length    => $max_bson_size,
            first_key     => '_id',
            first_value   => $id,
        }
    );

    # manually bless for speed
    return bless { bson => $bson_doc, metadata => { _id => $id } },
      "BSON::Raw";
}

1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
