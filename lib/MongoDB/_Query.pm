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

package MongoDB::_Query;

# Encapsulate query structure and modification

use version;
our $VERSION = 'v0.704.4.1';

use Moose;
use MongoDB::_Types;
use Tie::IxHash;
use namespace::clean -except => 'meta';

# XXX should all special fields just be attributes (or part of a "modifiers"
# attribute) and we assemble them on demand in an 'as_document' method?  That
# would allow easy coercion of things like sort documents to an ordered hash.
# It would also allow late validation e.g. "don't use snapshot with hint or
# orderby". If so, any '$' modifiers already in the spec need to be extracted
# during BUILD

has spec => (
    is       => 'ro',
    isa      => 'IxHash',
    required => 1,
    coerce   => 1,
    writer   => '_set_spec',
);

sub BUILD {
    my ($self) = @_;

    # if the first key is 'query' we must nest under the '$query' operator
    $self->_nest_query
      if $self->spec->Keys && $self->spec->Keys(0) eq 'query';
}

sub get_modifier {
    my ( $self, $key ) = @_;
    return $self->spec->FETCH($key);
}

sub set_modifier {
    my ( $self, $key, $value ) = @_;
    $self->_nest_query;
    $self->spec->STORE( $key, $value );
}

sub _nest_query {
    my ($self) = @_;
    # XXX this isn't quite right; we shouldn't nest query modifiers
    if ( !$self->spec->EXISTS('$query') ) {
        $self->_set_spec( Tie::IxHash->new( '$query' => $self->spec ) );
    }
    return;
}

sub query_doc {
    my ($self) = @_;
    # XXX copying IxHash is terribly inefficient
    my $ixhash =
      $self->spec->EXISTS('$query') ? $self->spec->FETCH('$query') : $self->spec;
    return Tie::IxHash->new( map { $_ => $ixhash->FETCH($_) } $ixhash->Keys );
}

sub clone {
    my ($self) = @_;
    my $ixhash = $self->spec;
    my $copy = Tie::IxHash->new( map { $_ => $ixhash->FETCH($_) } $ixhash->Keys );
    return ref($self)->new( spec => $copy );
}

1;
