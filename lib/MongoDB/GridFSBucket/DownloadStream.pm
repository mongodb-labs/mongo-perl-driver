#  Copyright 2015 - present MongoDB, Inc.
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
package MongoDB::GridFSBucket::DownloadStream;

# ABSTRACT: File handle abstraction for downloading

use version;
our $VERSION = 'v2.0.1';

use Moo;
use Types::Standard qw(
  Str
  Maybe
  HashRef
  InstanceOf
  FileHandle
);
use MongoDB::_Types qw(
  Boolish
  NonNegNum
);
use List::Util qw(max min);
use namespace::clean -except => 'meta';

=attr file_doc

The file document for the file to be downloaded.

Valid file documents typically include the following fields:

=for :list
* _id – a unique ID for this document, typically a L<BSON::OID> object.
  Legacy GridFS files may store this value as a different type.
* length – the length of this stored file, in bytes
* chunkSize – the size, in bytes, of each full data chunk of this file.
* uploadDate – the date and time this file was added to GridFS, stored as a
  BSON datetime value and inflated per the bucket's
  L<bson_codec|MongoDB::GridFSBucket/bson_codec> attribute.
* filename – the name of this stored file; this does not need to be unique
* metadata – any additional application-specific data
* md5 – DEPRECATED
* contentType – DEPRECATED
* aliases – DEPRECATED

=cut

has file_doc => (
    is       => 'ro',
    isa      => HashRef,
    required => 1,
);

has _buffer => (
    is  => 'rwp',
    isa => Str,
);

has _chunk_n => (
    is      => 'rwp',
    isa     => NonNegNum,
    default => 0,
);

has _result => (
    is       => 'ro',
    isa      => Maybe [ InstanceOf ['MongoDB::QueryResult'] ],
    required => 1,
);

# Currently this is always 0, but may be used to add
# optional rewinding in the future.
has _offset => (
    is      => 'rwp',
    isa     => NonNegNum,
    default => 0,
);

has _closed => (
    is      => 'rwp',
    isa     => Boolish,
    default => 0,
);

=method fh

    my $fh = $downloadstream->fh;
    while ( <$fh> ) {
        say($_);
    }

Returns a new Perl file handle tied to this instance of DownloadStream that
can be operated on with the built-in functions C<read>, C<readline>,
C<getc>, C<eof>, C<fileno> and C<close>.

B<Important notes>:

Allowing one of these tied filehandles to fall out of scope will NOT cause
close to be called. This is due to the way tied file handles are
implemented in Perl.  For close to be called implicitly, all tied
filehandles and the original object must go out of scope.

Each file handle retrieved this way is tied back to the same object, so
calling close on multiple tied file handles and/or the original object will
have the same effect as calling close on the original object multiple
times.

=cut

sub fh {
    my ($self) = @_;
    my $fh = IO::Handle->new();
    tie *$fh, 'MongoDB::GridFSBucket::DownloadStream', $self;
    return $fh;
}

sub _get_next_chunk {
    my ($self) = @_;

    return unless $self->_result && $self->_result->has_next;
    my $chunk = $self->_result->next;

    if ( $chunk->{'n'} != $self->_chunk_n ) {
        MongoDB::GridFSError->throw(
            sprintf(
                'ChunkIsMissing: expected chunk %d but got chunk %d',
                $self->_chunk_n, $chunk->{'n'},
            )
        );
    }

    my $last_chunk_n =
      int( $self->file_doc->{'length'} / $self->file_doc->{'chunkSize'} );
    my $expected_size =
        $chunk->{'n'} == $last_chunk_n
      ? $self->file_doc->{'length'} % $self->file_doc->{'chunkSize'}
      : $self->file_doc->{'chunkSize'};
    if ( length $chunk->{'data'} != $expected_size ) {
        MongoDB::GridFSError->throw(
            sprintf(
                "ChunkIsWrongSize: chunk %d from file with id %s has incorrect size %d, expected %d",
                $self->_chunk_n,
                $self->file_doc->{_id},
                length $chunk->{'data'},
                $expected_size,
            )
        );
    }

    $self->{_chunk_n} += 1;
    $self->{_buffer} .= $chunk->{data}->{data};
}

sub _ensure_buffer {
    my ($self) = @_;
    if ( $self->{_buffer} ) { return length $self->{_buffer} }

    $self->_get_next_chunk;

    return length $self->{_buffer};
}

sub _readline_scalar {
    my ($self) = @_;

    # Special case for "slurp" mode
    if ( !defined($/) ) {
        return $self->_read_all;
    }

    return unless $self->_ensure_buffer;
    my $newline_index;
    while ( ( $newline_index = index $self->{_buffer}, $/ ) < 0 ) {
        last unless $self->_get_next_chunk;
    }
    my $substr_len = $newline_index < 0 ? length $self->{_buffer} : $newline_index + 1;
    return substr $self->{_buffer}, $self->_offset, $substr_len, '';
}

sub _read_all {
    my ($self) = @_;

    if ( $self->_closed ) {
        warnings::warnif( 'closed',
            'read called on a closed MongoDB::GridFSBucket::DownloadStream' );
        return;
    }

    return unless $self->_result;

    my $chunk_size = $self->file_doc->{'chunkSize'};
    my $length = $self->file_doc->{'length'};
    my $last_chunk_n = int( $length / $chunk_size );
    my $last_chunk_size = $length % $chunk_size;

    my @chunks = $self->_result->all;

    for (my $i = 0; $i < @chunks; $i++ ) {
        my $n = $chunks[$i]{n};

        if ( $n != $i ) {
            MongoDB::GridFSError->throw(
                sprintf( 'ChunkIsMissing: expected chunk %d but got chunk %d', $i, $n)
            );
        }

        my $expected_size = ($n == $last_chunk_n ? $last_chunk_size : $chunk_size);
        if ( length $chunks[$i]{data}{data} != $expected_size ) {
            MongoDB::GridFSError->throw(
                sprintf(
                    "ChunkIsWrongSize: chunk %d of %d from file with id %s has incorrect size %d, expected %d",
                    $n,
                    $last_chunk_n,
                    $self->file_doc->{_id},
                    length $chunks[$i]{data}{data},
                    $expected_size,
                )
            );
        }
    }

    return join( "", map { $_->{data}{data} } @chunks );
}

=method close

    $stream->close

Works like the builtin C<close>.

B<Important notes:>

=for :list
* Calling close will also cause any tied file handles created for the
  stream to also close.
* C<close> will be automatically called when a stream object is destroyed.
* Calling C<close> repeatedly will warn.

=cut

sub close {
    my ($self) = @_;
    if ( $self->_closed ) {
        warn 'Attempted to close an already closed MongoDB::GridFSBucket::DownloadStream';
        return;
    }
    $self->_set__closed(1);
    $self->{_result} = undef;
    $self->{_buffer} = undef;
    $self->_set__chunk_n(0);
    return 1;
}

=method eof

    if ( $stream->eof() ) { ... }

Works like the builtin C<eof>.

=cut

sub eof {
    my ($self) = @_;
    return 1 if $self->_closed || !$self->_ensure_buffer;
    return;
}

=method fileno

    if ( $stream->fileno() ) { ... }

Works like the builtin C<fileno>, but it returns -1 if the stream is open
and undef if closed.

=cut

sub fileno {
    my ($self) = @_;
    return if $self->_closed;
    return -1;
}

=method getc

    $char = $stream->getc();

Works like the builtin C<getc>.

=cut

sub getc {
    my ($self) = @_;
    my $char;
    $self->read( $char, 1 );
    return $char;
}

=method read

    $data = $stream->read($buf, $length, $offset)

Works like the builtin C<read>.

=cut

sub read {
    my $self = shift;
    if ( $self->_closed ) {
        warnings::warnif( 'closed',
            'read called on a closed MongoDB::GridFSBucket::DownloadStream' );
        return;
    }
    my $buffref = \$_[0];
    my ( undef, $len, $offset ) = @_;
    if ( $len < 0 ) {
        MongoDB::UsageError->throw(
            'Negative length passed to MongoDB::GridFSBucket::DownloadStream->read');
    }
    my $bufflen = length $$buffref;

    $offset   ||= 0;
    $bufflen  ||= 0;
    $$buffref ||= '';

    $offset = max( 0, $bufflen + $offset ) if $offset < 0;
    if ( $offset > $bufflen ) {
        $$buffref .= ( "\0" x ( $offset - $bufflen ) );
    }
    else {
        substr $$buffref, $offset, $bufflen - $offset, '';
    }

    return 0 unless $self->_ensure_buffer;

    while ( length $self->{_buffer} < $len ) { last unless $self->_get_next_chunk }
    my $read_len = min( length $self->{_buffer}, $len );
    $$buffref .= substr $self->{_buffer}, $self->_offset, $read_len, '';
    return $read_len;
}

=method readline

    $line  = $stream->readline();
    @lines = $stream->readline();

Works like the builtin C<readline>.

=cut

sub readline {
    my ($self) = @_;
    if ( $self->_closed ) {
        warnings::warnif( 'closed',
            'readline called on a closed MongoDB::GridFSBucket::DownloadStream' );
        return;
    }
    return $self->_readline_scalar unless wantarray;

    my @result = ();
    while ( my $line = $self->_readline_scalar ) {
        push @result, $line;
    }
    return @result;
}

sub DEMOLISH {
    my ($self) = @_;
    $self->close unless $self->_closed;
}

# Magic tie methods

sub TIEHANDLE {
    my ( $class, $self ) = @_;
    return $self;
}

sub BINMODE {
    my ( $self, $mode ) = @_;
    if ( !$mode || $mode eq ':raw' ) {
        return 1;
    }
    $! = "binmode for " . __PACKAGE__ . " only supports :raw mode.";
    return
}

{
    no warnings 'once';
    *READ     = \&read;
    *READLINE = \&readline;
    *CLOSE    = \&close;
    *GETC     = \&getc;
    *EOF      = \&eof;
    *FILENO   = \&fileno;
}

my @unimplemented = qw(
  PRINT
  PRINTF
  SEEK
  TELL
  WRITE
);

for my $u (@unimplemented) {
    no strict 'refs';
    my $l = $u eq 'WRITE' ? 'syswrite' : lc($u);
    *{$u} = sub {
        MongoDB::UsageError->throw( "$l() not available on " . __PACKAGE__ );
    };
}

1;

__END__

=pod

=head1 SYNOPSIS

    # OO API
    $stream = $bucket->open_download_stream($file_id)
    while ( my $line = $stream->readline ) {
        ...
    }

    # Tied-handle API
    $fh = $stream->fh;
    while ( my $line = <$fh> ) {
        ...
    }

=head1 DESCRIPTION

This class provides a file abstraction for downloading.  You can stream
data from an object of this class using method calls or a tied-handle
interface.

=cut
