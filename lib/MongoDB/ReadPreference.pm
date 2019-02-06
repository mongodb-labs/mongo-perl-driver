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
package MongoDB::ReadPreference;

# ABSTRACT: Encapsulate and validate read preferences

use version;
our $VERSION = 'v2.1.1';

use Moo;
use MongoDB::Error;
use MongoDB::_Types qw(
    ArrayOfHashRef
    MaxStalenessNum
    NonNegNum
    ReadPrefMode
);
use namespace::clean -except => 'meta';

use overload (
    q[""]    => sub { $_[0]->mode },
    fallback => 1,
);

=attr mode

The read preference mode determines which server types are candidates
for a read operation.  Valid values are:

=for :list
* primary
* primaryPreferred
* secondary
* secondaryPreferred
* nearest

=cut

has mode => (
    is      => 'ro',
    isa     => ReadPrefMode,
    default => 'primary',
    coerce  => ReadPrefMode->coercion,
);

=attr tag_sets

The C<tag_sets> parameter is an ordered list of tag sets used to restrict the
eligibility of servers, such as for data center awareness.

The application of C<tag_sets> varies depending on the C<mode> parameter.  If
the C<mode> is 'primary', then C<tag_sets> must not be supplied.

=cut

has tag_sets => (
    is      => 'ro',
    isa     => ArrayOfHashRef,
    default => sub { [ {} ] },
    coerce  => ArrayOfHashRef->coercion,
);

=attr max_staleness_seconds

The C<max_staleness_seconds> parameter represents the maximum replication lag in
seconds (wall clock time) that a secondary can suffer and still be
eligible for reads. The default is -1, which disables staleness checks.

If the C<mode> is 'primary', then C<max_staleness_seconds> must not be supplied.

=cut

has max_staleness_seconds => (
    is => 'ro',
    isa => MaxStalenessNum,
    default => -1,
);

sub BUILD {
    my ($self) = @_;

    if ( $self->mode eq 'primary' && !$self->has_empty_tag_sets ) {
        MongoDB::UsageError->throw("A tag set list is not allowed with read preference mode 'primary'");
    }

    if ( $self->mode eq 'primary' && $self->max_staleness_seconds > 0 ) {
        MongoDB::UsageError->throw("A positive max_staleness_seconds is not allowed with read preference mode 'primary'");
    }

    return;
}

# Returns true if the C<tag_sets> array is empty or if it consists only of a
# single, empty hash reference.

sub has_empty_tag_sets {
    my ($self) = @_;
    my $tag_sets = $self->tag_sets;
    return @$tag_sets == 0 || ( @$tag_sets == 1 && !keys %{ $tag_sets->[0] } );
}

# Reformat to the document needed by mongos in $readPreference

sub _as_hashref {
    my ($self) = @_;
    return {
        mode => $self->mode,
        ( $self->has_empty_tag_sets ? () : ( tags => $self->tag_sets ) ),
        ( $self->max_staleness_seconds > 0 ? ( maxStalenessSeconds => int($self->max_staleness_seconds )) : () ),
    };
}

# Format as a string for error messages

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
    if ( $self->max_staleness_seconds > 0) {
        $string .= " ( maxStalenessSeconds: " . $self->max_staleness_seconds . " )";
    }
    return $string;
}


1;

__END__

=for Pod::Coverage has_empty_tag_sets for_mongos as_string

=head1 SYNOPSIS

    use MongoDB::ReadPreference;

    $rp = MongoDB::ReadPreference->new(); # mode: primary

    $rp = MongoDB::ReadPreference->new(
        mode     => 'primaryPreferred',
        tag_sets => [ { dc => 'useast' }, {} ],
    );

=head1 DESCRIPTION

A read preference indicates which servers should be used for read operations.

For core documentation on read preference see
L<http://docs.mongodb.org/manual/core/read-preference/>.

=head1 USAGE

Read preferences work via two attributes: C<mode> and C<tag_sets>.  The C<mode>
parameter controls the types of servers that are candidates for a read
operation as well as the logic for applying the C<tag_sets> attribute to
further restrict the list.

The following terminology is used in describing read preferences:

=for :list
* candidates – based on C<mode>, servers that could be suitable, based on
  C<tag_sets> and other logic
* eligible – these are candidates that match C<tag_sets>
* suitable – servers that meet all criteria for a read operation

=head2 Read preference modes

=head3 primary

Only an available primary is suitable.  C<tag_sets> do not apply and must not
be provided or an exception is thrown.

=head3 secondary

All secondaries (and B<only> secondaries) are candidates, but only eligible
candidates (i.e. after applying C<tag_sets>) are suitable.

=head3 primaryPreferred

Try to find a server using mode "primary" (with no C<tag_sets>).  If that
fails, try to find one using mode "secondary" and the C<tag_sets> attribute.

=head3 secondaryPreferred

Try to find a server using mode "secondary" and the C<tag_sets> attribute.  If
that fails, try to find a server using mode "primary" (with no C<tag_sets>).

=head3 nearest

The primary and all secondaries are candidates, but only eligible candidates
(i.e. after applying C<tag_sets> to all candidates) are suitable.

B<NOTE>: in retrospect, the name "nearest" is misleading, as it implies a
choice based on lowest absolute latency or geographic proximity, neither which
are true.

The "nearest" mode merely includes both primaries and secondaries without any
preference between the two.  All are filtered on C<tag_sets>.  Because of
filtering, servers might not be "closest" in any sense.  And if multiple
servers are suitable, one is randomly chosen based on the rules for L<server
selection|MongoDB::MongoClient/SERVER SELECTION>, which again might not be the
closest in absolute latency terms.

=head2 Tag set matching

The C<tag_sets> parameter is a list of tag sets (i.e. key/value pairs) to try
in order.  The first tag set in the list to match B<any> candidate server is
used as the filter for all candidate servers.  Any subsequent tag sets are
ignored.

A read preference tag set (C<T>) matches a server tag set (C<S>) – or
equivalently a server tag set (C<S>) matches a read preference tag set (C<T>) —
if C<T> is a subset of C<S> (i.e. C<T ⊆ S>).

For example, the read preference tag set C<< { dc => 'ny', rack => 2 } >>
matches a secondary server with tag set C<< { dc => 'ny', rack => 2, size =>
'large' } >>.

A tag set that is an empty document – C<< {} >> – matches any server, because
the empty tag set is a subset of any tag set.

=cut

