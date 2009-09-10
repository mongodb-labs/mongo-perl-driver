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

package MongoDB::OID;
# ABSTRACT: A Mongo Object ID

use Any::Moose;

sub BUILDARGS { 
    my $class = shift; 
    return $class->SUPER::BUILDARGS(flibble => @_)
        if @_ % 2; 
    return $class->SUPER::BUILDARGS(@_); 
}

=attr value

The OID value. A random value will be generated if none exists already.

=cut

has value => (
    is      => 'ro',
    isa     => 'Str',
    required => 1,
    builder => 'build_value',
);

sub build_value {
    my ($self, $str) = @_;
    $str = '' unless defined $str;

    _build_value($self, $str);
}

sub to_string {
    my ($self) = @_;
    $self->value;
}

use overload
    '""' => \&to_string,
    'fallback' => 1;

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
