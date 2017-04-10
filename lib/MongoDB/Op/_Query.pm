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

use strict;
use warnings;
package MongoDB::Op::_Query;

# Encapsulate a query operation; returns a MongoDB::QueryResult object

use version;
our $VERSION = 'v1.8.1';

use boolean;
use Moo;

use List::Util qw/min/;
use MongoDB::QueryResult;
use MongoDB::QueryResult::Filtered;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
    Document
    CursorType
    IxHash
);
use Types::Standard qw(
    CodeRef
    HashRef
    InstanceOf
    Maybe
    Bool
    Num
    Str
);
use boolean;

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

#--------------------------------------------------------------------------#
# Attributes based on the CRUD API spec: filter
#
# Some are mutable so that MongoDB::Cursor methods can manipulate them
# until the query is executed
#
# Unlike most parameters, these are camelCase so that find method options
# may be passed through directly.
#--------------------------------------------------------------------------#

# Immutable attributes

has filter => (
    is       => 'ro',
    isa      => Document,
    required => 1,
);

# Immutable attribute, but mutable hash.  We require it to be provided as
# we allow a private constructor so can't rely on a default.

has modifiers => (
    is  => 'ro',
    isa => HashRef,
    required => 1,
);

# Mutable attributes, due to legacy behavior of MongoDB::Cursor that allows
# modifying a deferred query operation before executing it.

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

has maxAwaitTimeMS => (
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

has skip => (
    is       => 'rw',
    isa      => Num,
    required => 1,
);

# optional attributes

has projection => (
    is  => 'rw',
    isa => Maybe( [Document] ),
);

has sort => (
    is  => 'rw',
    isa => Maybe( [IxHash] ),
);

has collation => (
    is  => 'rw',
    isa => Maybe( [Document] ),
);

# Not a MongoDB query attribute; this is used during construction of a
# result object
has post_filter => (
    is        => 'ro',
    predicate => 'has_post_filter',
    isa       => Maybe [CodeRef],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_ReadOp
  MongoDB::Role::_CommandCursorOp
  MongoDB::Role::_OpReplyParser
  MongoDB::Role::_ReadPrefModifier
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    if ( defined $self->collation and !$link->supports_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" );
    }

    my $res =
        $link->accepts_wire_version(4)
      ? $self->_command_query( $link, $topology )
      : $self->_legacy_query( $link, $topology );

    return $res;
}

sub _command_query {
    my ( $self, $link, $topology ) = @_;

    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => $self->as_command,
        query_flags     => {},
        read_preference => $self->read_preference,
        bson_codec      => $self->bson_codec,
    );
    my $res = $op->execute( $link, $topology );

    return $self->_build_result_from_cursor( $res );
}

sub _legacy_query {
    my ( $self, $link, $topology ) = @_;

    my $query_flags = {
        tailable => ( $self->cursorType =~ /^tailable/ ? 1 : 0 ),
        await_data => $self->cursorType eq 'tailable_await',
        immortal => $self->noCursorTimeout,
        partial => $self->allowPartialResults,
    };

    # build starting query document; modifiers come first as other parameters
    # take precedence.
    my $query = {
        ( $self->modifiers ? %{ $self->modifiers } : () ),
        ( $self->comment ? ( '$comment' => $self->comment ) : () ),
        ( $self->sort    ? ( '$orderby' => $self->sort )    : () ),
        (
              ( $self->maxTimeMS && $self->coll_name !~ /\A\$cmd/ )
            ? ( '$maxTimeMS' => $self->maxTimeMS )
            : ()
        ),
        '$query' => ($self->filter || {}),
    };

    # if no modifers were added and there is no 'query' key in '$query'
    # we remove the extra layer; this is necessary as some special
    # command queries will choke on '$query'
    # (see https://jira.mongodb.org/browse/SERVER-14294)
    $query = $query->{'$query'}
      if keys %$query == 1 && !(
        ( ref( $query->{'$query'} ) eq 'Tie::IxHash' )
        ? $query->{'$query'}->EXISTS('query')
        : exists $query->{'$query'}{query}
      );

    my $full_name  = $self->full_name;
    my $filter     = $self->bson_codec->encode_one( $query );

    # rules for calculating initial batch size
    my $limit      = $self->limit      || 0;
    my $batch_size = $self->batchSize || 0;
    my $n_to_return =
        $limit == 0      ? $batch_size
      : $batch_size == 0 ? $limit
      : $limit < 0       ? $limit
      :                    min( $limit, $batch_size );

    my $proj =
      $self->projection ? $self->bson_codec->encode_one( $self->projection ) : undef;

    # $query is passed as a reference because it *may* be replaced
    $self->_apply_read_prefs( $link, $topology, $query_flags, \$query);

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $full_name, $filter, $proj, $self->skip, $n_to_return,
        $query_flags );

    my $result =
      $self->_query_and_receive( $link, $op_bson, $request_id, $self->bson_codec );

    my $class =
      $self->has_post_filter ? "MongoDB::QueryResult::Filtered" : "MongoDB::QueryResult";

    return $class->_new(
        _client       => $self->client,
        _address      => $link->address,
        _full_name    => $full_name,
        _bson_codec   => $self->bson_codec,
        _batch_size   => $n_to_return,
        _cursor_at    => 0,
        _limit        => $self->limit,
        _cursor_id    => $result->{cursor_id},
        _cursor_start => $result->{starting_from},
        _cursor_flags => $result->{flags} || {},
        _cursor_num   => $result->{number_returned},
        _docs         => $result->{docs},
        _post_filter  => $self->post_filter,
    );
}

# awful hack: avoid calling into boolean to get true/false
my $TRUE = boolean::true();
my $FALSE = boolean::false();

sub as_command {
    my ($self) = @_;

    my ($limit, $batch_size, $single_batch) = ($self->{limit}, $self->{batchSize}, 0);

    $single_batch = $limit < 0 || $batch_size < 0;
    $limit = abs($limit);
    $batch_size = $limit if $single_batch;

    my $tailable = $self->{cursorType} =~ /^tailable/ ? $TRUE : $FALSE;
    my $await_data = $self->{cursorType} eq 'tailable_await' ? $TRUE : $FALSE;
    my $max_time = $await_data ? $self->{maxAwaitTimeMS} : $self->{maxTimeMS} ;

    my $mod = $self->{modifiers};

    return [
        find                => $self->{coll_name},
        filter              => $self->{filter},

        (defined $self->{sort} ? (sort => $self->{sort}) : ()),
        (defined $self->{projection} ? (projection => $self->{projection}) : ()),
        (defined $self->{collation} ? (collation => $self->{collation}) : ()),
        (defined $mod->{'$hint'} ? (hint => $mod->{'$hint'}) : ()),

        skip                => $self->{skip},

        ($limit ? (limit => $limit) : ()),
        ($batch_size ? (batchSize => $batch_size) : ()),

        singleBatch         => ($single_batch ? $TRUE : $FALSE),

        ($self->{comment} ? (comment => $self->{comment}) : ()),
        (defined $mod->{'$maxScan'} ? (maxScan => $mod->{'$maxScan'}) : ()),
        (defined $self->{maxTimeMS} ? (maxTimeMS => $self->{maxTimeMS}) : ()),
        (defined $mod->{'$max'} ? (max => $mod->{'$max'}) : ()),
        (defined $mod->{'$min'} ? (min => $mod->{'$min'}) : ()),
        (defined $mod->{'$returnKey'} ? (returnKey => $mod->{'$returnKey'}) : ()),
        (defined $mod->{'$showDiskLoc'} ? (showRecordId => $mod->{'$showDiskLoc'}) : ()),
        (defined $mod->{'$snapshot'} ? (snapshot => boolean($mod->{'$snapshot'})) : ()),

        tailable            => $tailable,
        noCursorTimeout     =>($self->{noCursorTimeout} ? $TRUE : $FALSE),
        awaitData           => $await_data,
        allowPartialResults =>($self->{allowPartialResults} ? $TRUE : $FALSE ),

        @{$self->{read_concern}->as_args},
    ];
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
