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

package MongoDB::Op::_Query;

# Encapsulate a query operation; returns a MongoDB::QueryResult object

use version;
our $VERSION = 'v0.999.999.7';

use Moo;

use MongoDB::BSON;
use MongoDB::QueryResult;
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

has db_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has coll_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

has projection => (
    is       => 'ro',
    isa      => Maybe [Document],
);

has [qw/batch_size limit skip/] => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

has sort => (
    is  => 'ro',
    isa => Maybe( [IxHash] ),
);

has filter => (
    is       => 'ro',
    isa      => Document,
);

has comment => (
    is       => 'ro',
    isa      => Str,
);

has max_time_ms => (
    is       => 'ro',
    isa      => Num,
);

has oplog_replay => (
    is       => 'ro',
    isa      => Bool,
);

has no_cursor_timeout => (
    is       => 'ro',
    isa      => Bool,
);

has allow_partial_results => (
    is       => 'ro',
    isa      => Bool,
);

has modifiers => (
    is  => 'ro',
    isa => HashRef,
);

has cursor_type => (
    is       => 'ro',
    isa      => CursorType,
);

has post_filter => (
    is        => 'ro',
    predicate => 'has_post_filter',
    isa       => Maybe [CodeRef],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_ReadOp
  MongoDB::Role::_CommandCursorOp
);
with 'MongoDB::Role::_LegacyReadPrefModifier';

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->accepts_wire_version(4)
      ? $self->_command_query( $link, $topology )
      : $self->_legacy_query( $link, $topology );

    return $res;
}

sub _command_query {
    my ( $self, $link, $topology ) = @_;

    my $cmd = $self->as_command;
    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => $cmd,
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
        tailable => ( $self->cursor_type =~ /^tailable/ ? 1 : 0 ),
        await_data => $self->cursor_type eq 'tailable_await',
        immortal => $self->no_cursor_timeout,
        partial => $self->allow_partial_results,
    };

    # build starting query document; modifiers come first as other parameters
    # take precedence.
    my $query = {
        ( $self->modifiers ? %{ $self->modifiers } : () ),
        ( $self->comment ? ( '$comment' => $self->comment ) : () ),
        ( $self->sort    ? ( '$orderby' => $self->sort )    : () ),
        (
              ( $self->max_time_ms && $self->coll_name !~ /\A\$cmd/ )
            ? ( '$maxTimeMS' => $self->max_time_ms )
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

    my $ns         = $self->db_name . "." . $self->coll_name;
    my $filter     = $self->bson_codec->encode_one( $query );
    my $batch_size = $self->limit || $self->batch_size;            # limit trumps

    my $proj =
      $self->projection ? $self->bson_codec->encode_one( $self->projection ) : undef;

    # $query is passed as a reference because it *may* be replaced
    $self->_apply_read_prefs( $link, $topology, $query_flags, \$query);

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $ns, $filter, $proj, $self->skip, $batch_size,
        $query_flags );

    my $result =
      $self->_query_and_receive( $link, $op_bson, $request_id, $self->bson_codec );

    my $class =
      $self->has_post_filter ? "MongoDB::QueryResult::Filtered" : "MongoDB::QueryResult";

    return $class->_new(
        _client       => $self->client,
        _address      => $link->address,
        _ns           => $ns,
        _bson_codec   => $self->bson_codec,
        _batch_size   => $batch_size,
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

sub as_command {
    my ($self) = @_;

    my ($limit, $batch_size, $single_batch) = ($self->limit, $self->batch_size, 0);

    if (defined $limit && $limit < 0) {
        $limit = abs($limit);
        $single_batch = true;
    }
    if (defined $batch_size && $batch_size < 0) {
        $batch_size = abs($batch_size);
        $single_batch = true;
    }

    my $tailable = $self->cursor_type =~ /^tailable/ ? true : false;
    my $await_data = $self->cursor_type eq 'tailable_await' ? true : false;

    my $mod = $self->modifiers;

    return Tie::IxHash->new(
        find                => $self->coll_name,
        filter              => $self->filter,

        defined $self->sort ? (sort => $self->sort) : (),
        defined $self->projection ? (projection => $self->projection) : (),
        defined $mod->{'$hint'} ? (hint => $mod->{'$hint'}) : (),

        skip                => $self->skip,

        $limit != 0 ? (limit => $limit) : (),
        $batch_size != 0 ? (batchSize => $batch_size) : (),

        singleBatch         => boolean($single_batch),
        comment             => $self->comment,

        defined $mod->{maxScan} ? (maxScan => $mod->{maxScan}) : (),

        maxTimeMS           => $self->max_time_ms,

        defined $mod->{max} ? (max => $mod->{max}) : (),
        defined $mod->{min} ? (min => $mod->{min}) : (),
        defined $mod->{returnKey} ? (returnKey => $mod->{returnKey}) : (),
        defined $mod->{showDiskLoc} ? (showRecordId => $mod->{showDiskLoc}) : (),
        defined $mod->{snapshot} ? (snapshot => $mod->{snapshot}) : (),

        tailable            => $tailable,
        oplogReplay         => boolean($self->oplog_replay),
        noCursorTimeout     => boolean($self->no_cursor_timeout),
        awaitData           => $await_data,
        allowPartialResults => boolean($self->allow_partial_results),
        #readConcern = ..., XXX unimplemented
    );
}

1;
