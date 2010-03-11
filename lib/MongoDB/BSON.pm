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
our $VERSION = '0.30_01';

# ABSTRACT: Tools for serializing and deserializing data in BSON form
use Any::Moose;

=head1 NAME

MongoDB::BSON - encoding and decoding utilities (more to come)

=head1 ATTRIBUTES

=head2 char

    $MongoDB::BSON::char = ":";
    $collection->query({"x" => {":gt" => 4}});

Can be used to set a character other than "$" to use for special operators.

=cut

$MongoDB::BSON::char = '$';

