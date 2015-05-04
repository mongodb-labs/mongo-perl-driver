#
#  Copyright 2009-2013 MongoDB, Inc.
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

package MongoDB::BSON;


# ABSTRACT: Tools for serializing and deserializing data in BSON form

use version;
our $VERSION = 'v0.999.998.6';

use XSLoader;
XSLoader::load("MongoDB", $VERSION);

use MongoDB::Error;
use Moose;
use namespace::clean -except => 'meta';

#--------------------------------------------------------------------------#
# legacy functions
#--------------------------------------------------------------------------#

sub decode_bson {
    my ($msg,$client) = @_;
    my @decode_args;
    if ( $client ) {
        @decode_args = map { $client->$_ } qw/dt_type inflate_dbrefs inflate_regexps/;
        push @decode_args, $client;
    }
    else {
        @decode_args = (undef, 0, 0, undef);
    }
    my $struct = eval { MongoDB::BSON::_legacy_decode_bson($msg, @decode_args) };
    MongoDB::ProtocolError->throw($@) if $@;
    return $struct;
}

sub encode_bson {
    my ($struct, $clean_keys, $max_size) = @_;
    $clean_keys = 0 unless defined $clean_keys;
    my $bson = eval { MongoDB::BSON::_legacy_encode_bson($struct, $clean_keys) };
    MongoDB::DocumentError->throw( message => $@, document => $struct) if $@;

    if ( $max_size && length($bson) > $max_size ) {
        MongoDB::DocumentError->throw(
            message => "Document exceeds maximum size $max_size",
            document => $struct,
        );
    }

    return $bson;
}

__PACKAGE__->meta->make_immutable;

1;

=for Pod::Coverage
decode_bson
encode_bson

=cut
