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
our $VERSION = 'v0.999.999.3'; # TRIAL

use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use MongoDB::Op::_Query;
use Syntax::Keyword::Junction qw/any/;
use Tie::IxHash;
use namespace::clean -except => 'meta';

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
    isa      => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

has bson_codec => (
    is       => 'ro',
    isa      => BSONCodec,
    required => 1,
);

has read_preference => (
    is       => 'rw',            # mutable for Cursor
    isa      => ReadPreference,
    required => 1,
    coerce   => 1,
);

#--------------------------------------------------------------------------#
# attributes based on the CRUD API spec: filter
#
# some are mutable so that MongoDB::Cursor methods can manipulate them
# until the query is executed
#--------------------------------------------------------------------------#

has filter => (
    is       => 'ro',
    isa      => IxHash,
    required => 1,
    coerce   => 1,
);

has modifiers => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

has allowPartialResults => (
    is  => 'rw',
    isa => Bool,
);

has batchSize => (
    is      => 'rw',
    isa     => Num,
    default => 0,
);

has comment => (
    is      => 'rw',
    isa     => Str,
    default => '',
);

has cursorType => (
    is      => 'rw',
    isa     => CursorType,
    default => 'non_tailable',
);

has limit => (
    is      => 'rw',
    isa     => Num,
    default => 0,
);

has maxTimeMS => (
    is      => 'rw',
    isa     => Num,
    default => 0,
);

has noCursorTimeout => (
    is  => 'rw',
    isa => Bool,
);

has oplogReplay => (
    is  => 'rw',
    isa => Bool,
);

has projection => (
    is      => 'rw',
    isa     => IxHash,
    coerce  => 1,
    default => sub { Tie::IxHash->new },
);

has skip => (
    is      => 'rw',
    isa     => Num,
    default => 0,
);

has sort => (
    is      => 'rw',
    isa     => IxHash,
    coerce  => 1,
    default => sub { Tie::IxHash->new },
);

sub as_query_op {
    my ( $self, $extra_params ) = @_;

    # construct query doc from filter, attributes and modifiers hash
    my $query = Tie::IxHash->new( '$query' => $self->filter );

    # modifiers go first
    while ( my ( $k, $v ) = each %{ $self->modifiers } ) {
        $query->STORE( $k, $v );
    }

    # if comment exists, it overwrites any earlier modifers
    if ( my $v = $self->comment ) {
        $query->STORE( '$comment' => $v );
    }

    # if maxTimeMS exists, it overwrites any earlier modifers
    if ( my $v = $self->maxTimeMS ) {
        # omit for $cmd* queries
        $query->STORE( '$maxTimeMS' => $v )
          unless $self->coll_name =~ /\A\$cmd/;
    }

    $query->STORE( '$orderby', $self->sort ) if $self->sort->Keys;

    # if no modifers were added and there is no 'query' key in '$query'
    # we don't need the extra layer
    if ( $query->Keys == 1 && !$query->FETCH('$query')->EXISTS('query') ) {
        $query = $query->FETCH('$query');
    }

    # construct query flags from attributes
    # XXX eventually flag names should get changed here and in _Protocol
    # to better match documentation or the CRUD API names
    my $query_flags = {
        tailable   => ($self->cursorType =~ /^tailable/ ? 1 : 0),
        await_data => $self->cursorType eq 'tailable_await',
        immortal   => $self->noCursorTimeout,
        partial    => $self->allowPartialResults,
    };

    # finally, generate the query op
    return MongoDB::Op::_Query->new(
        db_name     => $self->db_name,
        coll_name   => $self->coll_name,
        client      => $self->client,
        bson_codec  => $self->bson_codec,
        query       => $query,
        projection  => $self->projection,
        batch_size  => $self->batchSize,
        limit       => $self->limit,
        skip        => $self->skip,
        query_flags => $query_flags,
        ( $extra_params ? %$extra_params : () ),
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

    # deep copy IxHashes and modifiers
    for my $k (qw/filter projection sort/) {
        my $orig = $args{$k};
        my $copy = Tie::IxHash->new( map { $_ => $orig->FETCH($_) } $orig->Keys );
        $args{$k} = $copy;
    }
    $args{modifiers} = { %{ $args{modifiers} } };

    return ref($self)->new(%args);
}

1;
