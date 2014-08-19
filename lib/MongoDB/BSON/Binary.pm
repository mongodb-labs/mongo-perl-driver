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

package MongoDB::BSON::Binary;


# ABSTRACT: Binary type

use version;
our $VERSION = 'v0.704.5.1';

use Moose;
use namespace::clean -except => 'meta';

=head1 NAME

MongoDB::BSON::Binary - A type that can be used to send binary data to the
database

=head1 SYNOPSIS

Creates an instance of binary data with a specific subtype.

=head1 EXAMPLE

For example, suppose we wanted to store a profile pic.

    my $pic = MongoDB::BSON::Binary->new(data => $pic_bytes);
    $collection->insert({name => "profile pic", pic => $pic});

You can also, optionally, specify a subtype:

    my $pic = MongoDB::BSON::Binary->new(data => $pic_bytes,
        subtype => MongoDB::BSON::Binary->SUBTYPE_GENERIC);
    $collection->insert({name => "profile pic", pic => $pic});

=head1 SUBTYPES

MongoDB allows you to specify the "flavor" of binary data that you are storing
by providing a subtype.  The subtypes are purely cosmetic: the database treats
them all the same.

There are several subtypes defined in the BSON spec:

=over 4

=item C<SUBTYPE_GENERIC> (0x00) is the default used by the driver (as of 0.46).

=item C<SUBTYPE_FUNCTION> (0x01) is for compiled byte code.

=item C<SUBTYPE_GENERIC_DEPRECATED> (0x02) is deprecated. It was used by the
driver prior to version 0.46, but this subtype wastes 4 bytes of space so
C<SUBTYPE_GENERIC> is preferred.  This is the only type that is parsed
differently based on type.

=item C<SUBTYPE_UUID_DEPRECATED> (0x03) is deprecated.  It is for UUIDs.

=item C<SUBTYPE_UUID> (0x04) is for UUIDs.

=item C<SUBTYPE_MD5> can be (0x05) is for MD5 hashes.

=item C<SUBTYPE_USER_DEFINED> (0x80) is for user-defined binary types.

=back

=cut

use constant {
    SUBTYPE_GENERIC            => 0,
    SUBTYPE_FUNCTION           => 1,
    SUBTYPE_GENERIC_DEPRECATED => 2,
    SUBTYPE_UUID_DEPRECATED    => 3,
    SUBTYPE_UUID               => 4,
    SUBTYPE_MD5                => 5,
    SUBTYPE_USER_DEFINED       => 128
};

=head2 data

A string of binary data.

=cut

has data => (
    is => 'ro',
    isa => 'Str',
    required => 1
);

=head2 subtype

A subtype.  Defaults to C<SUBTYPE_GENERIC>.

=cut

has subtype => (
    is => 'ro',
    isa => 'Int',
    required => 0,
    default => MongoDB::BSON::Binary->SUBTYPE_GENERIC
);

=head2 Why is C<SUBTYPE_GENERIC_DEPRECATED> deprecated?

Binary data is stored with the length of the binary data, the subtype, and the
actually data.  C<SUBTYPE_GENERIC DEPRECATED> stores the length of the data a
second time, which just wastes four bytes.

If you have been using C<SUBTYPE_GENERIC_DEPRECATED> for binary data, moving to
C<SUBTYPE_GENERIC> should be painless: just use the driver normally and all
new/resaved data will be stored as C<SUBTYPE_GENERIC>.

It gets a little trickier if you've been querying by binary data fields:
C<SUBTYPE_GENERIC> won't match C<SUBTYPE_GENERIC_DEPRECATED>, even if the data
itself is the same.

=head2 Why is C<SUBTYPE_UUID_DEPRECATED> deprecated?

Other languages were using the UUID type to deserialize into their languages'
native UUID type.  They were doing this in different ways, so to standardize,
they decided on a deserialization format for everyone to use and changed the
subtype for UUID to the universal format.

This should not affect Perl users at all, as Perl does not deserialize it into
any native UUID type.

=cut

__PACKAGE__->meta->make_immutable;

1;

