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
our $VERSION = 'v0.999.999.4'; # TRIAL

use Config;
use Moo;
use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::Op::_GetMore;
use MongoDB::Op::_KillCursors;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean;

use constant {
    CURSOR_ZERO => "\0" x 8,
    HAS_INT64 => $Config{use64bitint},
};

with 'MongoDB::Role::_Cursor';

# attributes needed for get more

has _client => (
    is       => 'rw',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

has address => (
    is       => 'ro',
    required => 1,
    isa => HostAddress,
);

has ns => (
    is       => 'ro',
    required => 1,
    isa => Str,
);

has bson_codec => (
    is       => 'ro',
    required => 1,
    isa => BSONCodec,
);

has batch_size => (
    is      => 'ro',
    default => 0,
    isa => Int,
);

# attributes for tracking progress

has cursor_at => (
    is      => 'ro',
    default => 0,
    isa => Num,
);

sub _inc_cursor_at { $_[0]{cursor_at}++ }

has limit => (
    is      => 'ro',
    default => 0,
    isa => Num,
);

# attributes from actual results -- generally get this from BUILDARGS

has cursor_id => (
    is       => 'ro',
    required => 1,
    writer   => '_set_cursor_id',
    isa => Str,
);

has cursor_start => (
    is      => 'ro',
    default => 0,
    writer  => '_set_cursor_start',
    isa => Num,
);

has cursor_flags => (
    is      => 'ro',
    default => sub { {} },
    writer  => '_set_cursor_flags',
    isa => HashRef,
);

has cursor_num => (
    is      => 'ro',
    default => 0,
    isa => Num,
);

sub _inc_cursor_num { $_[0]{cursor_num}++ }

has _docs => (
    is      => 'ro',
    default => sub { [] },
    isa => ArrayRef,
);

sub _drained { ! @{$_[0]{_docs}} }
sub _doc_count { scalar @{$_[0]{_docs}} }
sub _add_docs {
    my $self = shift;
    push @{$self->{_docs}}, @_;
}
sub _next_doc { shift @{$_[0]{_docs}} }
sub _clear_doc { @{$_[0]{_docs}} = () }

# allows ->new( _client => $client, ns => $ns, reply => { } )
# or     ->new( _client => $client, result => $command_result )
sub BUILDARGS {
    my $self = shift;
    my $args = $self->SUPER::BUILDARGS(@_);

    if ( my $reply = delete $args->{reply} ) {
        # extract attributes from results hash
        return {
            %$args,
            cursor_id    => $reply->{cursor_id},
            cursor_flags => $reply->{flags},
            cursor_start => $reply->{starting_from},
            cursor_num   => $reply->{number_returned},
            _docs        => $reply->{docs},
        };
    }
    elsif ( my $cursor = delete $args->{cursor} ) {
        my $first_batch_size = scalar @{ $cursor->{firstBatch} };
        return {
            %$args,
            ns         => $cursor->{ns},
            batch_size => $first_batch_size,
            cursor_id  => _pack_cursor_id( $cursor->{id} ),
            cursor_num => $first_batch_size,
            _docs      => $cursor->{firstBatch},
        };
    }
    else {
        return $args;
    }

}

# for backward compatibility
sub started_iterating() { 1 }

# for backwards compatibility
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

=method has_next

    if ( $response->has_next ) {
        ...
    }

Returns true if additional documents are available.  This will
attempt to get another batch of documents from the server if
necessary.

=cut

sub has_next {
    my ($self) = @_;
    my $limit = $self->limit;
    if ( $limit > 0 && ( $self->cursor_at + 1 ) > $limit ) {
        $self->_kill_cursor;
        return 0;
    }
    return !$self->_drained || $self->_get_more;
}

=method next

    while ( $doc = $result->next ) {
        process_doc($doc)
    }

Returns the next document or C<undef> if the server cursor is exhausted.

=cut

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
        bson_codec => $self->bson_codec,
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

=method all

    @docs = $result->all;

Returns all documents as a list.

=cut

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
    return if !defined $self->cursor_id || $self->cursor_id eq CURSOR_ZERO;
    my $op = MongoDB::Op::_KillCursors->new( cursor_ids => [ $self->cursor_id ], );
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
    elsif (HAS_INT64) {
        # pack doesn't have endianness modifiers before perl 5.10.
        # We die during configuration on big-endian platforms on 5.8
        $cursor_id = pack( $] lt '5.010' ? "q" : "q<", $cursor_id );
    }
    else {
        # we on 32-bit perl *and* have a cursor ID that fits in 32 bits,
        # so pack it as long and pad out to a quad
        $cursor_id = pack( $] lt '5.010' ? "l" : "l<", $cursor_id ) . ( "\0" x 4 );
    }

    return $cursor_id;
}

=for Pod::Coverage
started_iterating
info

=head1 SYNOPSIS

    $cursor = $coll->find( $filter );
    $result = $cursor->result;

    while ( $doc = $result->next ) {
        process_doc($doc)
    }

=head1 DESCRIPTION

This class defines an iterator against a query result.  It automatically
fetches additional results from the originating mongod/mongos server
on demand.

For backwards compatibility reasons, L<MongoDB::Cursor> encapsulates query
parameters and generates a C<MongoDB::QueryResult> object on demand.  All
iterators on C<MongoDB::Cursor> delegate to C<MongoDB::QueryResult> object.

Retrieving this object and iterating on it directly will be slightly
more efficient.

=head1 USAGE

=head2 Error handling

Unless otherwise explictly documented, all methods throw exceptions if
an error occurs.  The error types are documented in L<MongoDB::Error>.

To catch and handle errors, the L<Try::Tiny> and L<Safe::Isa> modules
are recommended:

=head2 Cursor destruction

When a C<MongoDB::QueryResult> object is destroyed, a cursor termination
request will be sent to the originating server to free server resources.

=cut

1;
