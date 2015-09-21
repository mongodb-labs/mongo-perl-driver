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

package MongoDB::_Query;

# Encapsulate query structure and modification

use version;
our $VERSION = 'v0.999.999.7';

use Moo;
use MongoDB::_Types qw(
    BSONCodec
    Document
    ReadPreference
    CursorType
    IxHash
);

use Types::Standard qw(
    Str
    InstanceOf
    Maybe
    HashRef
    Bool
    Num
);

use MongoDB::Op::_Query;
use Tie::IxHash;
use namespace::clean;

#--------------------------------------------------------------------------#
# attributes for constructing/conducting the op
#--------------------------------------------------------------------------#

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has coll_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has client => (
    is       => 'ro',
    isa      => InstanceOf ['MongoDB::MongoClient'],
    required => 1,
);

has bson_codec => (
    is       => 'ro',
    isa      => BSONCodec,
    required => 1,
);

has read_preference => (
    is => 'rw',                   # mutable for Cursor
    isa => Maybe( [ReadPreference] ),
);

#--------------------------------------------------------------------------#
# attributes based on the CRUD API spec: filter
#
# some are mutable so that MongoDB::Cursor methods can manipulate them
# until the query is executed
#--------------------------------------------------------------------------#

has filter => (
    is       => 'ro',
    isa      => Document,
    required => 1,
);

# various things want to write here, so it must exist
has modifiers => (
    is  => 'ro',
    isa => HashRef,
    required => 1,
);

has allowPartialResults => (
    is       => 'rw',
    isa      => Bool,
    required => 1,
);

has batchSize => (
    is       => 'rw',
    isa      => Num,
    required => 1,
);

has comment => (
    is       => 'rw',
    isa      => Str,
    required => 1,
);

has cursorType => (
    is       => 'rw',
    isa      => CursorType,
    required => 1,
);

has limit => (
    is       => 'rw',
    isa      => Num,
    required => 1,
);

has maxTimeMS => (
    is       => 'rw',
    isa      => Num,
    required => 1,
);

has noCursorTimeout => (
    is       => 'rw',
    isa      => Bool,
    required => 1,
);

has oplogReplay => (
    is       => 'rw',
    isa      => Bool,
    required => 1,
);

has projection => (
    is  => 'rw',
    isa => Maybe( [Document] ),
);

has skip => (
    is       => 'rw',
    isa      => Num,
    required => 1,
);

has sort => (
    is  => 'rw',
    isa => Maybe( [IxHash] ),
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
);

sub as_query_op {
    my ( $self, $extra_params ) = @_;

    return MongoDB::Op::_Query->_new(
        db_name               => $self->db_name,
        coll_name             => $self->coll_name,
        client                => $self->client,
        bson_codec            => $self->bson_codec,
        filter                => $self->filter,
        projection            => $self->projection,
        batch_size            => $self->batchSize,
        limit                 => $self->limit,
        skip                  => $self->skip,
        'sort'                => $self->sort,
        comment               => $self->comment,
        max_time_ms           => $self->maxTimeMS,
        oplog_replay          => $self->oplogReplay,
        no_cursor_timeout     => $self->noCursorTimeout,
        allow_partial_results => $self->allowPartialResults,
        modifiers             => $self->modifiers,
        cursor_type           => $self->cursorType,
        read_preference       => $self->read_preference,
        exists $$extra_params{post_filter} ?
            (post_filter => $$extra_params{post_filter}) : (),
    );
}

sub execute {
    my ($self) = @_;
    return $self->client->send_read_op( $self->as_query_op );
}

sub clone {
    my ($self) = @_;

    # shallow copy everything;
    my %args = %$self;

    # deep copy any documents
    for my $k (qw/filter modifiers projection sort/) {
        my ($orig ) = $args{$k};
        next unless $orig;
        if ( ref($orig) eq 'Tie::IxHash' ) {
          $args{$k}= Tie::IxHash->new( map { $_ => $orig->FETCH($_) } $orig->Keys );
        }
        elsif ( ref($orig) eq 'ARRAY' ) {
         $args{$k}= [@$orig];
        }
        else {
         $args{$k} = { %$orig };
        }
    }

    return ref($self)->_new(%args);
}

1;
