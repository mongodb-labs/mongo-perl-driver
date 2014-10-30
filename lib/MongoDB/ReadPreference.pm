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

package MongoDB::ReadPreference;

# ABSTRACT: Encapsulate and validate read preferences

use version;
our $VERSION = 'v0.704.4.1';

use Moose;
use MongoDB::_Types;
use namespace::clean -except => 'meta';

use overload (
    q[""]    => sub { $_[0]->mode },
    fallback => 1,
);

has mode => (
    is      => 'ro',
    isa     => 'ReadPrefMode',
    default => 'primary',
    coerce  => 1,
);

has tag_sets => (
    is      => 'ro',
    isa     => 'ArrayOfHashRef',
    default => sub { [ {} ] },
    coerce  => 1,
);

sub BUILD {
    my ($self) = @_;

    if ( $self->mode eq 'primary' && !$self->has_empty_tag_sets ) {
        confess "A tag set list is not allowed with read preference mode 'primary'";
    }

    return;
}

sub has_empty_tag_sets {
    my ($self) = @_;
    my $tag_sets = $self->tag_sets;
    return @$tag_sets == 0 || ( @$tag_sets == 1 && !keys %{ $tag_sets->[0] } );
}

sub for_mongos {
    my ($self) = @_;
    return {
        mode => $self->mode,
        tags => $self->tag_sets,
    };
}

sub as_string {
    my ($self) = @_;
    my $string = $self->mode;
    unless ( $self->has_empty_tag_sets ) {
        my @ts;
        for my $set ( @{ $self->tag_sets } ) {
            push @ts, keys(%$set) ? join( ",", map { "$_\:$set->{$_}" } sort keys %$set ) : "";
        }
        $string .= " (" . join( ",", map { "{$_}" } @ts ) . ")";
    }
    return $string;
}

1;
