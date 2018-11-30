#  Copyright 2014 - present MongoDB, Inc.
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
package MongoDB::WriteConcern;

# ABSTRACT: Encapsulate and validate a write concern

use version;
our $VERSION = 'v2.0.3';

use Moo;
use MongoDB::Error;
use MongoDB::_Types qw(
    Boolish
);
use Types::Standard qw(
    ArrayRef
    Num
    Str
    Maybe
);
use Scalar::Util qw/looks_like_number/;
use boolean;
use namespace::clean -except => 'meta';

=attr w

Specifies the desired acknowledgement level. Defaults to '1'.

=cut

has w => (
    is        => 'ro',
    isa       => Maybe [Str],
    predicate => '_has_w',
);

=attr wtimeout

Specifies how long to wait for the write concern to be satisfied (in
milliseconds).  Defaults to 1000.

=cut

has wtimeout => (
    is        => 'ro',
    isa       => Num,
    predicate => '_has_wtimeout',
    default   => 1000,
);

=attr j

The j option confirms that the mongod instance has written the data to the
on-disk journal.  Defaults to false.

B<Note>: specifying a write concern that set j to a true value may result in an
error with a mongod or mongos running with --nojournal option now errors.

=cut

has j => (
    is        => 'ro',
    isa       => Boolish,
    predicate => '_has_j',
);

has _is_acknowledged => (
    is      => 'lazy',
    isa     => Boolish,
    reader  => 'is_acknowledged',
    builder => '_build_is_acknowledged',
);

has _as_args => (
    is      => 'lazy',
    isa     => ArrayRef,
    reader  => 'as_args',
    builder => '_build_as_args',
);

sub _build_is_acknowledged {
    my ($self) = @_;
    return !!( $self->j || $self->_w_is_acknowledged );
}

sub _build_as_args {
    my ($self) = @_;

    my $wc = {
        ( $self->_has_w        ? ( w        => $self->w )           : () ),
        ( $self->_has_wtimeout ? ( wtimeout => 0+ $self->wtimeout ) : () ),
        ( $self->_has_j        ? ( j        => boolean($self->j) )           : () ),
    };

    return ( (defined $self->w || defined $self->j) ? [writeConcern => $wc] : [] );
}

sub BUILD {
    my ($self) = @_;
    if ( ! $self->_w_is_acknowledged && $self->j ) {
        MongoDB::UsageError->throw("can't use write concern w=0 with j=" . $self->j );
    }
    return;
}

sub _w_is_acknowledged {
    my ($self) = @_;
    return ($self->_has_w
      && ( looks_like_number( $self->w ) ? $self->w > 0 : length $self->w ))
      || !defined $self->w;
}


1;

__END__

=head1 SYNOPSIS

    $rp = MongoDB::WriteConcern->new(); # w:1, wtimeout: 1000

    $rp = MongoDB::WriteConcern->new(
        w        => 'majority',
        wtimeout => 10000, # milliseconds
    );

=head1 DESCRIPTION

A write concern describes the guarantee that MongoDB provides when reporting on
the success of a write operation.

For core documentation on read preference see
L<http://docs.mongodb.org/manual/core/read-preference/>.

=cut
