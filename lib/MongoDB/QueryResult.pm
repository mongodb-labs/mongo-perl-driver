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
our $VERSION = 'v1.1.0';

use Moo;
use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::Op::_GetMore;
use MongoDB::Op::_KillCursors;
use MongoDB::_Types qw(
    BSONCodec
    HostAddress
);
use Types::Standard qw(
    Maybe
    ArrayRef
    Any
    InstanceOf
    Int
    HashRef
    Num
    Str
);
use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_Cursor
);

# attributes needed for get more

has _client => (
    is       => 'rw',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

has _address => (
    is       => 'ro',
    required => 1,
    isa => HostAddress,
);

has _ns => (
    is       => 'ro',
    required => 1,
    isa => Str,
);

has _bson_codec => (
    is       => 'ro',
    required => 1,
    isa => BSONCodec,
);

has _batch_size => (
    is       => 'ro',
    required => 1,
    isa      => Int,
);

has _max_time_ms => (
    is       => 'ro',
    isa      => Maybe[Num],
);

# attributes for tracking progress

has _cursor_at => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

sub _inc_cursor_at { $_[0]{_cursor_at}++ }

has _limit => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

# attributes from actual results

# integer or MongoDB::_CursorID or Math::BigInt
has _cursor_id => (
    is       => 'ro',
    required => 1,
    writer   => '_set_cursor_id',
    isa => Any,
);

has _cursor_start => (
    is       => 'ro',
    required => 1,
    writer   => '_set_cursor_start',
    isa      => Num,
);

has _cursor_flags => (
    is       => 'ro',
    required => 1,
    writer   => '_set_cursor_flags',
    isa      => HashRef,
);

has _cursor_num => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

sub _inc_cursor_num { $_[0]{_cursor_num}++ }

has _docs => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef,
);

sub _drained { ! @{$_[0]{_docs}} }
sub _doc_count { scalar @{$_[0]{_docs}} }
sub _add_docs {
    my $self = shift;
    push @{$self->{_docs}}, @_;
}
sub _next_doc { shift @{$_[0]{_docs}} }
sub _clear_doc { @{$_[0]{_docs}} = () }

# for backwards compatibility
sub started_iterating() { 1 }

sub _info {
    my ($self) = @_;
    return {
        flag      => $self->_cursor_flags,
        cursor_id => $self->_cursor_id,
        start     => $self->_cursor_start,
        at        => $self->_cursor_at,
        num       => $self->_cursor_num,
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
    my $limit = $self->_limit;
    if ( $limit > 0 && ( $self->_cursor_at + 1 ) > $limit ) {
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
    return 0 if $self->_cursor_id == 0;

    my $limit = $self->_limit;
    my $want = $limit > 0 ? ( $limit - $self->_cursor_at ) : $self->_batch_size;

    my ($db_name, $coll_name) = split(/\./, $self->_ns, 2);

    my $op = MongoDB::Op::_GetMore->_new(
        ns          => $self->_ns,
        db_name     => $db_name,
        coll_name   => $coll_name,
        client      => $self->_client,
        bson_codec  => $self->_bson_codec,
        cursor_id   => $self->_cursor_id,
        batch_size  => $want,
        max_time_ms => $self->_max_time_ms,
    );

    my $result = $self->_client->send_direct_op( $op, $self->_address );

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
    my $cursor_id = $self->_cursor_id;
    return if !defined $cursor_id || $cursor_id == 0;
    my $op = MongoDB::Op::_KillCursors->_new( cursor_ids => [ $cursor_id ], );
    $self->_client->send_direct_op( $op, $self->_address );
    $self->_set_cursor_id(0);
}

sub DEMOLISH {
    my ($self) = @_;
    $self->_kill_cursor;
}

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
