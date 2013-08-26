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

use Moose;

=head1 NAME

MongoDB::OID - A Mongo ObjectId

=head1 SYNOPSIS

If no C<_id> field is provided when a document is inserted into the database, an 
C<_id> field will be added with a new C<MongoDB::OID> as its value.

    my $id = $collection->insert({'name' => 'Alice', age => 20});

C<$id> will be a C<MongoDB::OID> that can be used to retreive or update the 
saved document:

    $collection->update({_id => $id}, {'age' => {'$inc' => 1}});
    # now Alice is 21

To create a copy of an existing OID, you must set the value attribute in the
constructor.  For example:

    my $id1 = MongoDB::OID->new;
    my $id2 = MongoDB::OID->new(value => $id1->value);
    my $id3 = MongoDB::OID->new($id1->value);
    my $id4 = MongoDB::OID->new($id1);

Now C<$id1>, C<$id2>, $<$id3> and C<$id4> will have the same value.

OID generation is thread safe.

=head1 SEE ALSO

Core documentation on object ids: L<http://dochub.mongodb.org/core/objectids>.

=head1 ATTRIBUTES

=head2 value

The OID value. A random value will be generated if none exists already.
It is a 24-character hexidecimal string (12 bytes).  

Its string representation is the 24-character string.

=cut

has value => (
    is      => 'ro',
    isa     => 'Str',
    required => 1,
    builder => 'build_value',
);

around BUILDARGS => sub {
    my $orig = shift;
    my $class = shift;
    if (@_ == 1) {
        return $class->$orig(value => $_[0])
            unless ref($_[0]);
        return $class->$orig(value => $_[0]->value)
            if blessed($_[0]) && $_[0]->isa($class);
    }
    return $class->$orig(@_);
};

sub build_value {
    my $self = shift;

    _build_value($self, @_ ? @_ : ());
}

=head1 METHODS

=head2 to_string

    my $hex = $oid->to_string;

Gets the value of this OID as a 24-digit hexidecimal string.

=cut

sub to_string {
    my ($self) = @_;
    $self->value;
}

=head2 get_time

    my $date = DateTime->from_epoch(epoch => $id->get_time);

Each OID contains a 4 bytes timestamp from when it was created.  This method
extracts the timestamp.  

=cut

sub get_time {
    my ($self) = @_;

    return hex(substr($self->value, 0, 8));
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


__PACKAGE__->meta->make_immutable;

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
