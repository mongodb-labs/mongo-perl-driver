#  Copyright 2014 - present MongoDB, Inc.
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

use strict;
use warnings;

package MongoDB::Op::_Query;

# Encapsulate a query operation; returns a MongoDB::QueryResult object

use version;
our $VERSION = 'v2.2.2';

use boolean;
use Moo;

use Scalar::Util qw/blessed/;
use List::Util qw/min/;
use MongoDB::QueryResult;
use MongoDB::QueryResult::Filtered;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
  Document
  CursorType
  IxHash
  to_IxHash
);
use Types::Standard qw(
  CodeRef
  HashRef
  InstanceOf
  Maybe
  Num
  Str
);

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

#--------------------------------------------------------------------------#
# Attributes based on the CRUD API spec: filter and options
#--------------------------------------------------------------------------#

has filter => (
    is       => 'ro',
    isa      => Document,
    required => 1,
);

# XXX The provided 'options' field *MUST* be the output of the class method
# 'precondition_options'.  Normally, we'd do this in a BUILD method, but in
# order to allow the use of the private constructor for speed, we push
# responsibility for conditioning the options to the calling site.

has options => (
    is       => 'ro',
    isa      => HashRef,
    required => 1,
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

    if ( defined $self->{options}{collation} and !$link->supports_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" );
    }

    my $res =
        $link->supports_query_commands
      ? $self->_command_query( $link, $topology )
      : $self->_legacy_query( $link, $topology );

    return $res;
}

sub _command_query {
    my ( $self, $link, $topology ) = @_;

    my $op = MongoDB::Op::_Command->_new(
        db_name             => $self->db_name,
        query               => $self->_as_command,
        query_flags         => {},
        read_preference     => $self->read_preference,
        bson_codec          => $self->bson_codec,
        session             => $self->session,
        monitoring_callback => $self->monitoring_callback,
    );
    my $res = $op->execute( $link, $topology );

    return $self->_build_result_from_cursor($res);
}

sub _legacy_query {
    my ( $self, $link, $topology ) = @_;

    my $opts = $self->{options};

    my $query_flags = {
        tailable => ( $opts->{cursorType} =~ /^tailable/ ? 1 : 0 ),
        await_data => $opts->{cursorType} eq 'tailable_await',
        immortal   => $opts->{noCursorTimeout},
        partial    => $opts->{allowPartialResults},
    };

    my $query = $self->_as_query_document($opts);

    my $full_name = $self->full_name;

    # rules for calculating initial batch size
    my $limit      = $opts->{limit}     // 0;
    my $batch_size = $opts->{batchSize} // 0;
    my $n_to_return =
        $limit == 0      ? $batch_size
      : $batch_size == 0 ? $limit
      : $limit < 0       ? $limit
      :                    min( $limit, $batch_size );

    my $proj =
      $opts->{projection} ? $self->bson_codec->encode_one( $opts->{projection} ) : undef;

    # $query is passed as a reference because it *may* be replaced
    $self->_apply_op_query_read_prefs( $link, $topology, $query_flags, \$query );

    my $filter = $self->bson_codec->encode_one($query);

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $full_name, $filter, $proj, $opts->{skip},
        $n_to_return, $query_flags );

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
        _limit        => $limit,
        _cursor_id    => $result->{cursor_id},
        _cursor_start => $result->{starting_from},
        _cursor_flags => $result->{flags} || {},
        _cursor_num   => $result->{number_returned},
        _docs         => $result->{docs},
        _post_filter  => $self->post_filter,
    );
}

# awful hack: avoid calling into boolean to get true/false
my $TRUE  = boolean::true();
my $FALSE = boolean::false();

sub _as_query_document {
    my ($self, $opts) = @_;

    # Reconstruct query modifiers style from options.  However, we only
    # apply $maxTimeMS if we're not running a command via OP_QUERY against
    # the '$cmd' collection.  For commands, we expect maxTimeMS to be in
    # the command itself.
    my $query = {
        ( defined $opts->{comment}      ? ( '$comment'     => $opts->{comment} )      : () ),
        ( defined $opts->{hint}         ? ( '$hint'        => $opts->{hint} )         : () ),
        ( defined $opts->{max}          ? ( '$max'         => $opts->{max} )          : () ),
        ( defined $opts->{min}          ? ( '$min'         => $opts->{min} )          : () ),
        ( defined $opts->{sort}         ? ( '$orderby'     => $opts->{sort} )         : () ),
        ( defined $opts->{maxScan}      ? ( '$maxScan'     => $opts->{maxScan} )      : () ),
        ( defined $opts->{returnKey}    ? ( '$returnKey'   => $opts->{returnKey} )    : () ),
        ( defined $opts->{showRecordId} ? ( '$showDiskLoc' => $opts->{showRecordId} ) : () ),
        ( defined $opts->{snapshot}     ? ( '$snapshot'    => $opts->{snapshot} )     : () ),
        (
              ( defined $opts->{maxTimeMS} && $self->coll_name !~ /\A\$cmd/ )
            ? ( '$maxTimeMS' => $opts->{maxTimeMS} )
            : ()
        ),
        # Not a user-provided option: this is only set by MongoDB::Op::_Explain
        # for legacy $explain support
        ( defined $opts->{explain} ? ( '$explain' => $TRUE ) : () ),
        ( '$query' => ( $self->filter || {} ) ),
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

    return $query;
}

my %options_to_prune =
  map { $_ => 1 } qw/limit batchSize cursorType maxAwaitTimeMS modifiers/;

sub _as_command {
    my ($self) = @_;

    my $opts = $self->{options};

    my $limit      = $opts->{limit}     // 0;
    my $batch_size = $opts->{batchSize} // 0;
    my $single_batch = $limit < 0 || $batch_size < 0;

    # find command always takes positive limit and batch size, so normalize
    # them based on rules in the "find, getmore, kill cursor" spec:
    # https://github.com/mongodb/specifications/blob/master/source/find_getmore_killcursors_commands.rst
    $limit = abs($limit);
    $batch_size = $limit if $single_batch;

    my $tailable = $opts->{cursorType} =~ /^tailable/ ? $TRUE : $FALSE;
    my $await_data = $opts->{cursorType} eq 'tailable_await' ? $TRUE : $FALSE;

    return [
        # Always send these options
        find        => $self->{coll_name},
        filter      => $self->{filter},
        tailable    => $tailable,
        awaitData   => $await_data,
        singleBatch => ( $single_batch ? $TRUE : $FALSE ),
        @{ $self->{read_concern}->as_args( $self->session ) },

        ( $limit      ? ( limit     => $limit )      : () ),
        ( $batch_size ? ( batchSize => $batch_size ) : () ),

        # Merge in any server options, but cursorType and maxAwaitTimeMS aren't
        # actually a server option, so we remove it during the merge.  Also
        # remove limit and batchSize as those may have been modified

        ( map { $_ => $opts->{$_} } grep { !exists $options_to_prune{$_} } keys %$opts )
    ];
}

# precondition_options is a class method that, given query options,
# combines keys from the deprecated 'modifiers' option with the correct
# precedence.  It provides defaults and and coerces values if needed.
#
# It returns a hash reference with extracted and coerced options.
sub precondition_options {
    my ( $class, $opts ) = @_;
    $opts //= {};
    my $mods = $opts->{modifiers} // {};
    my %merged = (

        #
        # Keys always included in commands or used in calcuations need a
        # default value if not provided.
        #

        # integer
        ( skip => $opts->{skip} // 0 ),

        # boolean
        ( allowPartialResults => ( $opts->{allowPartialResults} ? $TRUE : $FALSE ) ),

        # boolean
        ( noCursorTimeout => ( $opts->{noCursorTimeout} ? $TRUE : $FALSE ) ),

        # integer
        ( batchSize => $opts->{batchSize} // 0 ),

        # integer
        ( limit => $opts->{limit} // 0 ),

        # string
        ( cursorType => $opts->{cursorType} // 'non_tailable' ),

        #
        # These are optional keys that should be included only if defined.
        #

        # integer
        (
            defined $opts->{maxAwaitTimeMS} ? ( maxAwaitTimeMS => $opts->{maxAwaitTimeMS} ) : ()
        ),

        # hashref
        ( defined $opts->{projection} ? ( projection => $opts->{projection} ) : () ),

        # hashref
        ( defined $opts->{collation} ? ( collation => $opts->{collation} ) : () ),

        #
        # These keys have equivalents in the 'modifiers' option: if an options
        # key exists it takes precedence over a modifiers key, but undefined
        # values disable the option in both cases.
        #

        # string
        (
              ( exists $opts->{comment} )
            ? ( ( defined $opts->{comment} ) ? ( comment => $opts->{comment} ) : () )
            : (
                  ( defined $mods->{'$comment'} )
                ? ( comment => $mods->{'$comment'} )
                : ()
            )
        ),

        # string or ordered document
        (
              ( exists $opts->{hint} )
            ? ( ( defined $opts->{hint} ) ? ( hint => $opts->{hint} ) : () )
            : (
                  ( defined $mods->{'$hint'} )
                ? ( hint => $mods->{'$hint'} )
                : ()
            )
        ),

        # ordered document
        (
              ( exists $opts->{max} )
            ? ( ( defined $opts->{max} ) ? ( max => $opts->{max} ) : () )
            : (
                  ( defined $mods->{'$max'} )
                ? ( max => $mods->{'$max'} )
                : ()
            )
        ),

        # ordered document
        (
              ( exists $opts->{min} )
            ? ( ( defined $opts->{min} ) ? ( min => $opts->{min} ) : () )
            : (
                  ( defined $mods->{'$min'} )
                ? ( min => $mods->{'$min'} )
                : ()
            )
        ),

        # integer
        (
              ( exists $opts->{maxScan} )
            ? ( ( defined $opts->{maxScan} ) ? ( maxScan => $opts->{maxScan} ) : () )
            : (
                  ( defined $mods->{'$maxScan'} )
                ? ( maxScan => $mods->{'$maxScan'} )
                : ()
            )
        ),

        # integer
        (
              ( exists $opts->{maxTimeMS} )
            ? ( ( defined $opts->{maxTimeMS} ) ? ( maxTimeMS => $opts->{maxTimeMS} ) : () )
            : (
                  ( defined $mods->{'$maxTimeMS'} )
                ? ( maxTimeMS => $mods->{'$maxTimeMS'} )
                : ()
            )
        ),

        # ordered document

        (
              ( exists $opts->{sort} )
            ? ( ( defined $opts->{sort} ) ? ( sort => $opts->{sort} ) : () )
            : (
                  ( defined $mods->{'$orderby'} )
                ? ( sort => $mods->{'$orderby'} )
                : ()
            )
        ),

        # boolean
        (
              ( exists $opts->{returnKey} )
            ? ( ( defined $opts->{returnKey} ) ? ( returnKey => $opts->{returnKey} ) : () )
            : (
                  ( defined $mods->{'$returnKey'} )
                ? ( returnKey => $mods->{'$returnKey'} )
                : ()
            )
        ),

        # boolean
        (
            ( exists $opts->{showRecordId} )
            ? (
                ( defined $opts->{showRecordId} ) ? ( showRecordId => $opts->{showRecordId} ) : () )
            : (
                  ( defined $mods->{'$showDiskLoc'} )
                ? ( showRecordId => $mods->{'$showDiskLoc'} )
                : ()
            )
        ),

        # boolean
        (
              ( exists $opts->{snapshot} )
            ? ( ( defined $opts->{snapshot} ) ? ( snapshot => $opts->{snapshot} ) : () )
            : (
                  ( defined $mods->{'$snapshot'} )
                ? ( snapshot => $mods->{'$snapshot'} )
                : ()
            )
        ),
    );

    # coercions to IxHash: unrolled for efficiency
    $merged{sort} = to_IxHash( $merged{sort} ) if exists $merged{sort};
    $merged{max}  = to_IxHash( $merged{max} )  if exists $merged{max};
    $merged{min}  = to_IxHash( $merged{min} )  if exists $merged{min};

    # optional coercion to IxHash if hint is a reference type
    $merged{hint} = to_IxHash( $merged{hint} ) if ref $merged{hint};

    # coercions to boolean (if not already coerced): unrolled for efficiency
    $merged{returnKey} = ( $merged{returnKey} ? $TRUE : $FALSE )
      if exists $merged{returnKey};
    $merged{showRecordId} = ( $merged{showRecordId} ? $TRUE : $FALSE )
      if exists $merged{showRecordId};
    $merged{snapshot} = ( $merged{snapshot} ? $TRUE : $FALSE )
      if exists $merged{snapshot};

    return \%merged;
}

# Setters are provided to support the MongoDB::Cursor interface that modifies
# options prior to execution.  These methods preserve the rules for each key
# that are used in precondition_options.  Specifically, if passed *undef*,
# the options are cleared, except for options that must have a default.

# setters for boolean options
for my $key ( qw/returnKey showRecordId snapshot/ ) {
    no strict 'refs';
    my $method = "set_$key";
    *{$method} = sub {
        my ($self,$value) = @_;
        if ( defined $value ) {
            $self->{options}{$key} = $value ? $TRUE : $FALSE;
        }
        else {
            delete $self->{options}{$key};
        }
    }
}

# setters for scalar & hashref options
for my $key ( qw/collation comment maxAwaitTimeMS maxScan maxTimeMS projection/ ) {
    no strict 'refs';
    my $method = "set_$key";
    *{$method} = sub {
        my ($self,$value) = @_;
        if ( defined $value ) {
            $self->{options}{$key} = $value;
        }
        else {
            delete $self->{options}{$key};
        }
    }
}

# setters for ordered document options
for my $key ( qw/max min sort/ ) {
    no strict 'refs';
    my $method = "set_$key";
    *{$method} = sub {
        my ($self,$value) = @_;
        if ( defined $value ) {
            $self->{options}{$key} = to_IxHash($value);
        }
        else {
            delete $self->{options}{$key};
        }
    }
}

# setter for hint, which is an ordered document *or* scalar
sub set_hint {
    my ($self,$value) = @_;
    if ( defined $value ) {
        $self->{options}{hint} = ref $value ? to_IxHash($value) : $value;
    }
    else {
        delete $self->{options}{hint};
    }
}

# setters with default of 0
for my $key ( qw/batchSize limit skip/ ) {
    no strict 'refs';
    my $method = "set_$key";
    *{$method} = sub {
        my ($self,$value) = @_;
        $self->{options}{$key} = $value // 0;
    }
}

# setters with default of $FALSE
for my $key ( qw/allowPartialResults noCursorTimeout/ ) {
    no strict 'refs';
    my $method = "set_$key";
    *{$method} = sub {
        my ($self,$value) = @_;
        $self->{options}{$key} = $value ? $TRUE : $FALSE;
    }
}

# cursorType has a specific default value
sub set_cursorType {
    my ($self,$value) = @_;
    $self->{options}{cursorType} = $value // 'non_tailable';
}

sub has_hint {
    my ($self) = @_;
    return $self->{options}{hint};
}

1;
