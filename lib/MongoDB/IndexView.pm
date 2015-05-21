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
use Types::Standard qw/-types slurpy/;
use Type::Params qw/compile/;
use boolean;
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

has _bson_codec => (
    is      => 'ro',
    isa     => BSONCodec,
    lazy    => 1,
    builder => '_build__bson_codec',
);

sub _build__bson_codec {
    my ($self) = @_;
    return $self->collection->bson_codec;
}

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

has _coll_name => (
    is      => 'ro',
    isa     => Str,
    lazy    => 1,
    builder => '_build__coll_name',
);

sub _build__coll_name {
    my ($self) = @_;
    return $self->collection->name;
}

has _db_name => (
    is      => 'ro',
    isa     => Str,
    lazy    => 1,
    builder => '_build__db_name',
);

sub _build__db_name {
    my ($self) = @_;
    return $self->collection->database->name;
}

#--------------------------------------------------------------------------#
# public methods
#--------------------------------------------------------------------------#

=method list

    $result = $indexes->list;

    while ( my $index = $result->next ) {
        ...
    }

    for my $index ( $result->all ) {
        ...
    }

This method returns a L<MongoDB::QueryResult> which can be used to
retrieve index information either one at a time (with C<next>) or
all at once (with C<all>).

=cut

my $list_args;

sub list {
    $list_args ||= compile(Object);
    my ($self) = $list_args->(@_);

    my $op = MongoDB::Op::_ListIndexes->new(
        client     => $self->_client,
        db_name    => $self->_db_name,
        coll_name  => $self->_coll_name,
        bson_codec => $self->_bson_codec,
    );

    return $self->_client->send_read_op($op);
}

=method create_one

=cut

my $create_one_args;

sub create_one {
    $create_one_args ||= compile( Object, IxHash, Optional( [HashRef] ) );
    my ( $self, $keys, $opts ) = $create_one_args->(@_);
}

=method create_many

    @names = $indexes->create_many(
        { keys => [ x => 1, y => 1 ] },
        { keys => [ z => 1 ], options => { unique => 1 } }
    );

This method takes a list of index models (given as hash references)
and returns a list of index names created.  It will throw an exception
on error.

Each index module is described by the following fields:

=for :list
* C<keys> (required) — an ordered document (array reference or
  L<Tie::IxHash> object) with an ordered list of index keys and index
  directions.  See below for more.
* C<options> — an optional hash reference of index options.

The C<keys> document needs to be ordered.  While it can take a hash
reference, because Perl randomizes the order of hash keys, you should
B<ONLY> use a hash reference with a single-key index.  You are B<STRONGLY>
encouraged to get in the habit of specifying index keys with an array
reference.

The form of the C<keys> document differs based on the type of index (e.g.
single-key, multi-key, text, geospatial, etc.).  See
L<Index Types|http://docs.mongodb.org/manual/core/index-types/> in the
MongoDB Manual for specifics.

The C<options> hash reference may have a mix of general-purpose and
index-type-specific options.  See L<Index
Options|http://docs.mongodb.org/manual/reference/method/db.collection.createIndex/#options>
in the MongoDB Manual for specifics.  Some of the most frequently used keys
include:

=for :list
* background — when true, index creation won't block but will run in the
  background; this is strongly recommended to avoid blocking other
  operations on the database.
* unique — enforce uniqueness; inserting a duplicate document (or creating
  one with update modifiers) will raise an error.
* name — a name for the index; one will be generated if this is omitted.

=cut

my $create_many_args;

sub create_many {
    $create_many_args ||= compile( Object,
        slurpy ArrayRef [ Dict [ keys => Ref, options => Optional [HashRef] ] ] );
    my ( $self, $models ) = $create_many_args->(@_);

    my $indexes = [ map __flatten_index_model($_), @$models ];
    my $op = MongoDB::Op::_CreateIndexes->new(
        db_name       => $self->_db_name,
        coll_name     => $self->_coll_name,
        bson_codec    => $self->_bson_codec,
        indexes       => $indexes,
        write_concern => MongoDB::WriteConcern->new,
    );

    # succeed or die; we don't care about response document
    $self->_client->send_write_op($op);

    return map $_->{name}, @$indexes;
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

#--------------------------------------------------------------------------#
# private functions
#--------------------------------------------------------------------------#

sub __flatten_index_model {
    my ($model) = @_;

    my ( $keys, $orig ) = @{$model}{qw/keys options/};

    # copy the original so we don't modify it
    my $opts = { $orig ? %$orig : () };

    for my $k (qw/keys key/) {
        MongoDB::UsageError->throw("Can't specify '$k' in options to index creation")
          if exists $opts->{$k};
    }

    # add name if not provided
    $opts->{name} = __to_index_string($keys)
      unless defined $opts->{name};

    # convert some things to booleans
    for my $k (qw/unique background sparse dropDups/) {
        next unless exists $opts->{$k};
        $opts->{$k} = boolean( $opts->{$k} );
    }

    # return is document ready for the createIndexes command
    return { key => $keys, %$opts };
}

# utility function to generate an index name by concatenating key/value pairs
sub __to_index_string {
    my $keys = shift;

    my @name;
    if ( ref $keys eq 'ARRAY' ) {
        @name = @$keys;
    }
    elsif ( ref $keys eq 'HASH' ) {
        @name = %$keys;
    }
    elsif ( ref $keys eq 'Tie::IxHash' ) {
        my @ks = $keys->Keys;
        my @vs = $keys->Values;

        for ( my $i = 0; $i < $keys->Length; $i++ ) {
            push @name, $ks[$i];
            push @name, $vs[$i];
        }
    }
    else {
        MongoDB::UsageError->throw(
            "expected Tie::IxHash, hash, or array reference for keys");
    }

    return join( "_", @name );
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
