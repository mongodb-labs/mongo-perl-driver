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

package MongoDB::WriteConcern;

# ABSTRACT: Encapsulate and validate a write concern

use version;
our $VERSION = 'v0.999.998.2'; # TRIAL

use Moose;
use MongoDB::Error;
use MongoDB::_Types;
use Scalar::Util qw/looks_like_number/;
use namespace::clean -except => 'meta';

has w => (
    is        => 'ro',
    isa       => 'Str',
    predicate => '_has_w',
    default   => 1,
);

has wtimeout => (
    is        => 'ro',
    isa       => 'Num',
    predicate => '_has_wtimeout',
    default   => 1000,
);

has j => (
    is        => 'ro',
    isa       => 'booleanpm',
    coerce    => 1,
    predicate => '_has_j',
);

has is_safe => (
    is      => 'ro',
    isa     => 'Bool',
    lazy    => 1,
    builder => '_build_is_safe',
);

has as_struct => (
    is      => 'ro',
    isa     => 'HashRef',
    lazy    => 1,
    builder => '_build_as_struct',
);

sub _build_is_safe {
    my ($self) = @_;
    return !!( $self->j || $self->_w_is_safe );
}

sub _build_as_struct {
    my ($self) = @_;
    return {
        ( $self->_has_w        ? ( w        => $self->w )        : () ),
        ( $self->_has_wtimeout ? ( wtimeout => $self->wtimeout ) : () ),
        ( $self->_has_j        ? ( j        => $self->j )        : () ),
    };
}

sub BUILD {
    my ($self) = @_;
    if ( ! $self->_w_is_safe && $self->j ) {
        MongoDB::Error->throw("can't use write concern w=0 with j=" . $self->j );
    }
    return;
}

sub _w_is_safe {
    my ($self) = @_;
    return $self->_has_w
      && ( looks_like_number( $self->w ) ? $self->w > 0 : length $self->w );
}

1;
