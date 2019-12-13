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
package MongoDB::Role::_UpdatePreEncoder;

# MongoDB interface for pre-encoding and validating update/replace docs

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use BSON::Raw;
use BSON::Types 'bson_array';
use MongoDB::Error;
use MongoDB::_Constants;

use namespace::clean;

requires qw/bson_codec/;

sub _pre_encode_update {
    my ( $self, $max_bson_object_size, $doc, $is_replace ) = @_;

    my $type = ref($doc);
    my $bson_doc;

    if ( $type eq 'BSON::Raw' ) {
        $bson_doc = $doc->bson;
    }
    elsif ($type eq 'BSON::Array' && !$is_replace) {
        return $doc;
    }
    else {
        if ($type eq 'ARRAY' && scalar(@$doc) && ref($doc->[0]) && !$is_replace) {
            return bson_array(@$doc);
        }
        else {
            $bson_doc = $self->bson_codec->encode_one(
                $doc,
                {
                    invalid_chars => $is_replace ? '.' : '',
                    max_length => $is_replace ? $max_bson_object_size : undef,
                }
            );
        }
    }

    # must check if first character of first key is valid for replace/update;
    # do this from BSON to get key *after* op_char replacment;
    # only need to validate if length is enough for a document with a key

    my ( $len, undef, $first_char ) = unpack( P_INT32 . "CZ", $bson_doc );
    if ( $len >= MIN_KEYED_DOC_LENGTH ) {
        my $err;
        if ($is_replace) {
            $err = "replacement document must not contain update operators"
                if $first_char eq '$';
        }
        else {
            $err = "update document must only contain update operators"
                if $first_char ne '$';
        }

        MongoDB::DocumentError->throw(
            message  => $err,
            document => $doc,
        ) if $err;
    }
    elsif ( ! $is_replace ) {
        MongoDB::DocumentError->throw(
            message  => "Update document was empty!",
            document => $doc,
        );
    }

    return $doc if $type eq 'BSON::Raw';
    # manually bless for speed
    return bless { bson => $bson_doc, metadata => {} }, "BSON::Raw";
}

1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
