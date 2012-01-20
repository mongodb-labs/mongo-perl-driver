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

package MongoDB::Code;
our $VERSION = '0.45';

# ABSTRACT: JavaScript Code

=head1 NAME

MongoDB::Code - JavaScript code

=cut

use Any::Moose;

=head1 ATTRIBUTES

=head2 code

A string of JavaScript code.

=cut

has code => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

=head2 scope

An optional hash of variables to pass as the scope.

=cut

has scope => (
    is       => 'ro',
    isa      => 'HashRef',
    required => 0,
);

1;
