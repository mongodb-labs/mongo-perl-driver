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

package MongoDB::Timestamp;


# ABSTRACT: Replication timestamp

use version;
our $VERSION = 'v0.704.5.1';

=head1 NAME

MongoDB::Timestamp - Timestamp used for replication

=head1 SYNOPSIS

This is an internal type used for replication.  It is not for storing dates,
times, or timestamps in the traditional sense.  Unless you are looking to mess
with MongoDB's replication internals, the class you are probably looking for is
L<DateTime>.  See <MongoDB::DataTypes> for more information.

=cut

use Moose;
use namespace::clean -except => 'meta';

=head1 ATTRIBUTES

=head2 sec

Seconds since epoch.

=cut

has sec => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
);

=head2 inc

Incrementing field.

=cut

has inc => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
);

__PACKAGE__->meta->make_immutable;

1;
