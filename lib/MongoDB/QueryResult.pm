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

package MongoDB::QueryResult;

# ABSTRACT: An iterator for Mongo query results

use version;
our $VERSION = 'v0.999.998.2'; # TRIAL

use Moose;
use MongoDB::Op::_GetMore;
use MongoDB::Op::_KillCursors;
use MongoDB::_Types;
use namespace::clean -except => 'meta';

use constant { CURSOR_ZERO => "\0" x 8, };

with 'MongoDB::Role::_Cursor';

# attributes needed for get more

has _client => (
    is       => 'rw',
    isa      => 'MongoDB::MongoClient',
    required => 1,
);

has address => (
    is       => 'ro',
    isa      => 'HostAddress',
    required => 1,
);

has ns => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has batch_size => (
    is      => 'ro',
    isa     => 'Int',
    default => 0,
);

# attributes for tracking progress

has cursor_at => (
    is      => 'ro',
    isa     => 'Num',
    traits  => ['Counter'],
    default => 0,
    handles => {
        _inc_cursor_at   => 'inc',
        _clear_cursor_at => 'reset',
    },
);

has limit => (
    is      => 'ro',
    isa     => 'Num',
    default => 0,
);

# attributes from actual results -- generally get this from BUILDARGS

has cursor_id => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    writer   => '_set_cursor_id',
);

has cursor_start => (
    is      => 'ro',
    isa     => 'Num',
    default => 0,
    writer  => '_set_cursor_start',
);

has cursor_flags => (
    is      => 'ro',
    isa     => 'HashRef',
    default => sub { {} },
    writer  => '_set_cursor_flags',
);

has cursor_num => (
    is      => 'ro',
    isa     => 'Num',
    traits  => ['Counter'],
    default => 0,
    handles => {
        _inc_cursor_num   => 'inc',
        _clear_cursor_num => 'reset',
    },
);

has _docs => (
    is      => 'ro',
    isa     => 'ArrayRef',
    traits  => ['Array'],
    default => sub { [] },
    handles => {
        _drained    => 'is_empty',
        _doc_count  => 'count',
        _add_docs   => 'push',
        _next_doc   => 'shift',
        _clear_docs => 'clear',
    },
);

# allows ->new( _client => $client, ns => $ns,
sub BUILDARGS {
    my $self = shift;
    my $args = $self->SUPER::BUILDARGS(@_);

    if ( my $result = delete $args->{result} ) {
        # extract attributes from results hash
        return {
            %$args,
            cursor_id    => $result->{cursor_id},
            cursor_flags => $result->{flags},
            cursor_start => $result->{starting_from},
            cursor_num   => $result->{number_returned},
            _docs        => $result->{docs},
        };
    }
    else {
        return $args;
    }

}

# for backward compatibility
sub started_iterating() { 1 }

sub info {
    my ($self) = @_;
    return {
        flag      => $self->cursor_flags,
        cursor_id => $self->cursor_id,
        start     => $self->cursor_start,
        at        => $self->cursor_at,
        num       => $self->cursor_num,
    };
}

sub has_next {
    my ($self) = @_;
    my $limit = $self->limit;
    if ( $limit > 0 && ( $self->cursor_at + 1 ) > $limit ) {
        $self->_kill_cursor;
        return 0;
    }
    return 1 if !$self->_drained;
    return $self->_get_more;
}

sub next {
    my ($self) = @_;
    return unless $self->has_next;
    $self->_inc_cursor_at();
    return $self->_next_doc;
}

sub _get_more {
    my ($self) = @_;
    return 0 if $self->cursor_id eq CURSOR_ZERO;

    my $limit = $self->limit;
    my $want = $limit > 0 ? ( $limit - $self->cursor_at ) : $self->batch_size;

    my $op = MongoDB::Op::_GetMore->new(
        ns         => $self->ns,
        client     => $self->_client,
        bson_codec => $self->_client,  # XXX for now
        cursor_id  => $self->cursor_id,
        batch_size => $want,
    );

    my $result = $self->_client->send_direct_op( $op, $self->address );

    $self->_set_cursor_id( $result->{cursor_id} );
    $self->_set_cursor_flags( $result->{flags} );
    $self->_set_cursor_start( $result->{starting_from} );
    $self->_inc_cursor_num( $result->{number_returned} );
    $self->_add_docs( @{ $result->{docs} } );
    return scalar @{ $result->{docs} };
}

sub all {
    my ($self) = @_;
    my @ret;

    while ( my $entry = $self->next ) {
        push @ret, $entry;
    }

    return @ret;
}

sub _kill_cursor {
    my ($self) = @_;
    return if $self->cursor_id eq CURSOR_ZERO;
    my $op = MongoDB::Op::_KillCursors->new(
        cursor_ids => [ $self->cursor_id ],
    );
    $self->_client->send_direct_op( $op, $self->address );
    $self->_set_cursor_id(CURSOR_ZERO);
}

sub DEMOLISH {
    my ($self) = @_;
    $self->_kill_cursor;
}

#--------------------------------------------------------------------------#
# utility functions
#--------------------------------------------------------------------------#

# If we get a cursor_id from a command, BSON decoding will give us either
# a perl scalar or a Math::BigInt object (if we don't have 32 bit support).
# For OP_GET_MORE, we treat it as an opaque string, so we need to convert back
# to a packed, little-endian quad
sub _pack_cursor_id {
    my $cursor_id = shift;
    if ( ref($cursor_id) eq "Math::BigInt" ) {
        my $as_hex = $cursor_id->as_hex; # big-endian hex
        substr( $as_hex, 0, 2, '' );     # remove "0x"
        my $len = length($as_hex);
        substr( $as_hex, 0, 0, "0" x ( 16 - $len ) ) if $len < 16; # pad to quad length
        $cursor_id = pack( "H*", $as_hex );                        # packed big-endian
        $cursor_id = reverse($cursor_id);                          # reverse to little-endian
    }
    else {
        # pack doesn't have endianness modifiers before perl 5.10.
        # We die during configuration on big-endian platforms on 5.8
        $cursor_id = pack( $] lt '5.010' ? "q" : "q<", $cursor_id );
    }
    return $cursor_id;
}

__PACKAGE__->meta->make_immutable;

1;
