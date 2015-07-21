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

package MongoDB::OID;

# ABSTRACT: A Mongo Object ID

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use MongoDB::BSON;
use Moo;
use MongoDB;
use MongoDB::_Constants;
use Types::Standard -types;
use namespace::clean;

=head1 ATTRIBUTES

=head2 value

The OID value. A random value will be generated if none exists already.
It is a 24-character hexidecimal string (12 bytes).

Its string representation is the 24-character string.

=cut

has value => (
    is      => 'ro',
    required => 1,
    builder => '_build_value',
    isa => Str,
);

# XXX need to set up typedef with str length
# msg: "OIDs need to have a length of 24 bytes"

sub _build_value {
    my ($self) = @_;
    return MongoDB::BSON::generate_oid();
}

around BUILDARGS => sub {
    my $orig = shift;
    my $class = shift;
    if ( @_ == 0 ) {
        return { value => MongoDB::BSON::generate_oid() };
    }
    if ( @_ == 1 ) {
        return { value => "$_[0]" };
    }
    return $orig->($class, @_);
};

# This private constructor bypasses everything Moo does for us and just
# jams an OID into a blessed hashref.  This is only for use in super-hot
# code paths, like document insertion.
sub _new_oid {
    return bless { value => MongoDB::BSON::generate_oid() }, $_[0];
}

=head1 METHODS

=head2 to_string

    my $hex = $oid->to_string;

Gets the value of this OID as a 24-digit hexidecimal string.

=cut

sub to_string { $_[0]->{value} }

=head2 get_time

    my $date = DateTime->from_epoch(epoch => $id->get_time);

Each OID contains a 4 bytes timestamp from when it was created.  This method
extracts the timestamp.

=cut

sub get_time {
    my ($self) = @_;

    return hex(substr($self->value, 0, 8));
}

# for testing purposes
sub _get_pid {
    my ($self) = @_;

    return hex(substr($self->value, 14, 4));
}

=head2 TO_JSON

    my $json = JSON->new;
    $json->allow_blessed;
    $json->convert_blessed;

    $json->encode(MongoDB::OID->new);

Returns a JSON string for this OID.  This is compatible with the strict JSON
representation used by MongoDB, that is, an OID with the value
"012345678901234567890123" will be represented as
C<{"$oid" : "012345678901234567890123"}>.

=cut

sub TO_JSON {
    my ($self) = @_;
    return {'$oid' => $self->value};
}

use overload
    '""' => \&to_string,
    'fallback' => 1;

1;

=head1 SYNOPSIS

If no C<_id> field is provided when a document is inserted into the database, an
C<_id> field will be added with a new C<MongoDB::OID> as its value.

    my $id = $collection->insert({'name' => 'Alice', age => 20});

C<$id> will be a C<MongoDB::OID> that can be used to retrieve or update the
saved document:

    $collection->update({_id => $id}, {'age' => {'$inc' => 1}});
    # now Alice is 21

To create a copy of an existing OID, you must set the value attribute in the
constructor.  For example:

    my $id1 = MongoDB::OID->new;
    my $id2 = MongoDB::OID->new(value => $id1->value);
    my $id3 = MongoDB::OID->new($id1->value);
    my $id4 = MongoDB::OID->new($id1);

Now C<$id1>, C<$id2>, C<$id3> and C<$id4> will have the same value.

OID generation is thread safe.

=head1 SEE ALSO

Core documentation on object ids: L<http://dochub.mongodb.org/core/objectids>.

=cut
