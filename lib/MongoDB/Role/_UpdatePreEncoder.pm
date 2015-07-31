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

package MongoDB::Role::_UpdatePreEncoder;

# MongoDB interface for pre-encoding and validating update/replace docs

use version;
our $VERSION = 'v0.999.999.5';

use Moo::Role;
use MongoDB::Error;
use namespace::clean;

use constant PERL58 => $] lt '5.010';

use constant {
    P_INT32 => PERL58 ? "l" : "l<",
    MIN_KEYED_DOC_LENGTH => 8,
};

requires qw/bson_codec/;

sub _pre_encode_update {
    my ( $self, $link, $doc, $is_replace ) = @_;

    my $bson_doc = $self->bson_codec->encode_one(
        $doc,
        {
            invalid_chars => $is_replace ? '.' : '',
            max_length => $is_replace ? $link->max_bson_object_size : undef,
        }
    );

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


    return MongoDB::BSON::_EncodedDoc->_new(
        bson => $bson_doc,
        metadata => {},
    );
}

1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
