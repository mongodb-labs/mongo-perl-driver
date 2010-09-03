#
#  Copyright 2009 10gen, Inc.
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
our $VERSION = '0.37';

# ABSTRACT: Tools for serializing and deserializing data in BSON form
use Any::Moose;

=head1 NAME

MongoDB::BSON - Encoding and decoding utilities (more to come)

=head1 ATTRIBUTES

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
