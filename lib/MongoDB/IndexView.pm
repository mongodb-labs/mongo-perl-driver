#
#  Copyright 2015 MongoDB, Inc.
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

package MongoDB::IndexView;

# ABSTRACT: Index management for a collection

use version;
our $VERSION = 'v0.999.998.7'; # TRIAL

use Moose;
use MongoDB::Error;
use MongoDB::WriteConcern;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

=attr collection

The L<MongoDB::Collection> for which indexes are being created or viewed.

=cut

#--------------------------------------------------------------------------#
# constructor attributes
#--------------------------------------------------------------------------#

has collection => (
    is       => 'ro',
    isa      => InstanceOf( ['MongoDB::Collection'] ),
    required => 1,
);

#--------------------------------------------------------------------------#
# private attributes
#--------------------------------------------------------------------------#

has _client => (
    is      => 'ro',
    isa     => InstanceOf( ['MongoDB::MongoClient'] ),
    lazy    => 1,
    builder => '_build__client',
);

sub _build__client {
    my ($self) = @_;
    return $self->collection->client;
}

#--------------------------------------------------------------------------#
# public methods
#--------------------------------------------------------------------------#

=method list

=cut

sub list {
    my ($self) = @_;
}

=method create_one

=cut

my $create_one_args;

sub create_one {
    $create_one_args ||= compile( Object, IxHash, Optional( [HashRef] ) );
    my ( $self, $keys, $opts ) = $create_one_args->(@_);
}

=method create_many

=cut

my $create_many_args;

sub create_many {
    $create_many_args ||= compile( Object, ArrayOfHashRef );
    my ( $self, $models ) = $create_many_args->(@_);
}

=method drop_one

=cut

my $drop_one_args;

sub drop_one {
    $drop_one_args ||= compile( Object, Str );
    my ( $self, $name ) = $drop_one_args->(@_);
}

=method drop_all

=cut

sub drop_all {
    my ($self) = @_;
}

__PACKAGE__->meta->make_immutable;

1;

=head1 SYNOPSIS

    my $indexes = $collection->indexes;

=head1 DESCRIPTION

This class models the indexes on a L<MongoDB::Collection> so you can
create, list or drop them.

For more on MongoDB indexes, see the L<MongoDB Manual pages on
indexing|http://docs.mongodb.org/manual/core/indexes/>

=cut

# vim: set ts=4 sts=4 sw=4 et tw=75:
