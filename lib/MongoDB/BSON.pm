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
our $VERSION = 'v0.704.4.1';

use XSLoader;
XSLoader::load("MongoDB", $VERSION);

use Moose;
use namespace::clean -except => 'meta';

=head1 NAME

MongoDB::BSON - Encoding and decoding utilities (more to come)

=head1 ATTRIBUTES

=head2 C<looks_like_number>

    $MongoDB::BSON::looks_like_number = 1;
    $collection->insert({age => "4"}); # stores 4 as an int

If this is set, the driver will be more aggressive about converting strings into
numbers.  Anything that L<Scalar::Util>'s looks_like_number would approve as a
number will be sent to MongoDB as its numeric value.

Defaults to 0 (for backwards compatibility).

If you do not set this, you may be using strings more often than you intend to.
See the L<MongoDB::DataTypes> section for more info on the behavior of strings
vs. numbers.

=cut

$MongoDB::BSON::looks_like_number = 0;

=head2 char

    $MongoDB::BSON::char = ":";
    $collection->query({"x" => {":gt" => 4}});

Can be used to set a character other than "$" to use for special operators.

=cut

$MongoDB::BSON::char = '$';

=head2 Turn on/off UTF8 flag when return strings

    # turn off utf8 flag on strings
    $MongoDB::BSON::utf8_flag_on = 0;

Default is turn on, that compatible with version before 0.34.

If set to 0, will turn of utf8 flag on string attribute and return on bytes mode, meant same as :

    utf8::encode($str)

Currently MongoDB return string with utf8 flag, on character mode , some people
wish to turn off utf8 flag and return string on byte mode, it maybe help to display "pretty" strings.

NOTE:

If you turn off utf8 flag, the string  length will compute as bytes, and is_utf8 will return false.

=cut

$MongoDB::BSON::utf8_flag_on = 1;

=head2 Return boolean values as booleans instead of integers

    $MongoDB::BSON::use_boolean = 1

By default, booleans are deserialized as integers.  If you would like them to be
deserialized as L<boolean/true> and L<boolean/false>, set
C<$MongoDB::BSON::use_boolean> to 1.

=cut

$MongoDB::BSON::use_boolean = 0;

=head2 Return binary data as instances of L<MongoDB::BSON::Binary> instead of
string refs.

    $MongoDB::BSON::use_binary = 1

For backwards compatibility, binary data is deserialized as a string ref.  If
you would like to have it deserialized as instances of L<MongoDB::BSON::Binary>
(to, say, preserve the subtype), set C<$MongoDB::BSON::use_binary> to 1.

=cut

$MongoDB::BSON::use_binary = 0;

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
    my $struct = eval { MongoDB::BSON::_decode_bson($msg, @decode_args) };
    Carp::confess($@) if $@;
    return $struct;
}

sub encode_bson {
    my ($struct, $clean_keys) = @_;
    $clean_keys = 0 unless defined $clean_keys;
    my $bson = eval { MongoDB::BSON::_encode_bson($struct, $clean_keys) };
    Carp::confess($@) if $@;
    return $bson;
}

__PACKAGE__->meta->make_immutable;

1;
