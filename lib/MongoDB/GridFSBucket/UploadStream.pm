#
#  Copyright 2009-2015 MongoDB, Inc.
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
package MongoDB::GridFSBucket::UploadStream;

# ABSTRACT: File handle abstraction for uploading

use version;
our $VERSION = 'v1.999.0';

use Moo;
use BSON::OID;
use BSON::Time;
use Encode;
use MongoDB::Error;
use MongoDB::BSON::Binary;
use Time::HiRes qw/time/;
use Types::Standard qw(
  Str
  Maybe
  HashRef
  ArrayRef
  InstanceOf
);
use MongoDB::_Types qw(
  Boolish
  NonNegNum
);
use MongoDB::_Constants;
use Digest::MD5;
use bytes;
use namespace::clean -except => 'meta';

=attr chunk_size_bytes

The number of bytes per chunk.  Defaults to the C<chunk_size_bytes> of the
originating bucket object.

This will be stored in the C<chunkSize> field of the file document on
a successful upload.

=cut

has chunk_size_bytes => (
    is      => 'ro',
    isa     => NonNegNum,
    default => 255 * 1024,
);

=attr filename

The filename to store the file under. Note that filenames are NOT necessarily unique.

This will be stored in the C<filename> field of the file document on
a successful upload.

=cut

has filename => (
    is  => 'ro',
    isa => Str,
);

=attr metadata

An optional hashref for storing arbitrary metadata about the file.

If defined, this will be stored in the C<metadata> field of the file
document on a successful upload.

=cut

has metadata => (
    is  => 'ro',
    isa => Maybe [HashRef],
);

=attr content_type (DEPRECATED)

An optional MIME type. This field should only be used for backwards
compatibility with older GridFS implementations. New applications should
store the content type in the metadata hash if needed.

If defined, this will be stored in the C<contentType> field of the file
document on a successful upload.

=cut

has content_type => (
    is  => 'ro',
    isa => Str,
);

=attr aliases (DEPRECATED)

An optional array of aliases. This field should only be used for backwards
compatibility with older GridFS implementations. New applications should
store aliases in the metadata hash if needed.

If defined, this will be stored in the C<aliases> field of the file
document on a successful upload.

=cut

has aliases => (
    is  => 'ro',
    isa => ArrayRef [Str],
);

has _bucket => (
    is       => 'ro',
    isa      => InstanceOf ['MongoDB::GridFSBucket'],
    required => 1,
);

=method id

    $id = $stream->id;

The id of the file created by the stream.  It will be stored in the C<_id>
field of the file document on a successful upload.  Some upload methods
require specifying an id at upload time.  Defaults to a newly-generated
L<BSON::OID> or BSON codec specific equivalent.

=cut

has id => (
    is  => 'lazy',
);

sub _build_id {
    my $self = shift;
    my $creator = $self->_bucket->bson_codec->can("create_oid");
    return $creator ? $creator->() : BSON::OID->new();
}

has _closed => (
    is      => 'rwp',
    isa     => Boolish,
    default => 0,
);

has _buffer => (
    is      => 'rwp',
    isa     => Str,
    default => '',
);

has _length => (
    is      => 'rwp',
    isa     => NonNegNum,
    default => 0,
);

has _md5 => (
    is  => 'lazy',
    isa => InstanceOf ['Digest::MD5'],
);

sub _build__md5 {
    return Digest::MD5->new;
}

has _chunk_buffer_length => (
    is  => 'lazy',
    isa => NonNegNum,
);

sub _build__chunk_buffer_length {
    my ($self) = @_;
    my $docsize = $self->chunk_size_bytes + 36;
    return MAX_GRIDFS_BATCH_SIZE - $docsize;
}

has _current_chunk_n => (
    is      => 'rwp',
    isa     => NonNegNum,
    default => 0,
);

=method fh

    my $fh = $stream->fh;
    print $fh, 'test data...';
    close $fh

Returns a new file handle tied to this instance of UploadStream that can be
operated on with the built-in functions C<print>, C<printf>, C<syswrite>,
C<fileno> and C<close>.

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
    tie *$fh, 'MongoDB::GridFSBucket::UploadStream', $self;
    return $fh;
}

sub _flush_chunks {
    my ( $self, $all ) = @_;
    my @chunks = ();
    my $data;
    while ( length $self->{_buffer} >= $self->chunk_size_bytes
        || ( $all && length $self->{_buffer} > 0 ) )
    {
        $data = substr $self->{_buffer}, 0, $self->chunk_size_bytes, '';

        push @chunks,
          {
            files_id => $self->id,
            n        => int( $self->_current_chunk_n ),
            data     => MongoDB::BSON::Binary->new( { data => $data } ),
          };
        $self->{_current_chunk_n} += 1;
    }
    if ( scalar(@chunks) ) {
        eval { $self->_bucket->_chunks->insert_many( \@chunks ) };
        if ($@) {
            MongoDB::GridFSError->throw("Error inserting chunks: $@");
        }
    }
}

sub _write_data {
    my ( $self, $data ) = @_;
    Encode::_utf8_off($data); # force it to bytes for transmission
    $self->{_buffer} .= $data;
    $self->{_length} += length $data;
    $self->_md5->add($data);
    $self->_flush_chunks if length $self->{_buffer} >= $self->_chunk_buffer_length;
}

=method abort

    $stream->abort;

Aborts the upload by deleting any chunks already uploaded to the database
and closing the stream.

=cut

sub abort {
    my ($self) = @_;
    if ( $self->_closed ) {
        warn 'Attempted to abort an already closed UploadStream';
        return;
    }

    $self->_bucket->_chunks->delete_many( { files_id => $self->id } );
    $self->_set__closed(1);
}

=method close

    $file_doc = $stream->close;

Closes the stream and flushes any remaining data to the database. Once this is
done a file document is created in the GridFS bucket, making the uploaded file
visible in subsequent queries or downloads.

On success, the file document hash reference is returned as a convenience.

B<Important notes:>

=for :list
* Calling close will also cause any tied file handles created for the
  stream to also close.
* C<close> will be automatically called when a stream object is destroyed.
  When called this way, any errors thrown will not halt execution.
* Calling C<close> repeately will warn.

=cut

sub close {
    my ($self) = @_;
    if ( $self->_closed ) {
        warn 'Attempted to close an already closed MongoDB::GridFSBucket::UploadStream';
        return;
    }
    $self->_flush_chunks(1);
    my $filedoc = {
        _id        => $self->id,
        length     => $self->_length,
        chunkSize  => $self->chunk_size_bytes,
        uploadDate => BSON::Time->new(),
        md5        => $self->_md5->hexdigest,
        filename   => $self->filename,
    };
    $filedoc->{'contentType'} = $self->content_type if $self->content_type;
    $filedoc->{'metadata'}    = $self->metadata     if $self->metadata;
    $filedoc->{'aliases'}     = $self->aliases      if $self->aliases;
    eval { $self->_bucket->_files->insert_one($filedoc) };
    if ($@) {
        MongoDB::GridFSError->throw("Error inserting file document: $@");
    }
    $self->_set__closed(1);
    return $filedoc;
}

=method fileno

    if ( $stream->fileno ) { ... }

Works like the builtin C<fileno>, but it returns -1 if the stream is open
and undef if closed.

=cut

sub fileno {
    my ($self) = @_;
    return if $self->_closed;
    return -1;
}

=method print

    $stream->print(@data);

Works like the builtin C<print>.

=cut

sub print {
    my $self = shift;
    return if $self->_closed;
    my $fsep = defined($,) ? $, : '';
    my $osep = defined($\) ? $\ : '';
    my $output = join( $fsep, @_ ) . $osep;
    $self->_write_data($output);
    return 1;
}

=method printf

    $stream->printf($format, @data);

Works like the builtin C<printf>.

=cut

sub printf {
    my $self   = shift;
    my $format = shift;
    local $\;
    $self->print( sprintf( $format, @_ ) );
}

=method syswrite

    $stream->syswrite($buffer);
    $stream->syswrite($buffer, $length);
    $stream->syswrite($buffer, $length, $offset);

Works like the builtin C<syswrite>.

=cut

sub syswrite {
    my ( $self, $buff, $len, $offset ) = @_;
    my $bufflen = length $buff;

    $len = $bufflen unless defined $len;
    if ( $len < 0 ) {
        MongoDB::UsageError->throw(
            'Negative length passed to MongoDB::GridFSBucket::DownloadStream->read');
    }

    $offset ||= 0;

    local $\;
    $self->print( substr( $buff, $offset, $len ) );
}

sub DEMOLISH {
    my ($self) = @_;
    $self->close unless $self->_closed;
}

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
    *PRINT  = \&print;
    *PRINTF = \&printf;
    *WRITE  = \&syswrite;
    *CLOSE  = \&close;
    *FILENO = \&fileno;
}

my @unimplemented = qw(
  EOF
  GETC
  READ
  READLINE
  SEEK
  TELL
);

for my $u (@unimplemented) {
    no strict 'refs';
    my $l = lc($u);
    *{$u} = sub {
        MongoDB::UsageError->throw( "$l() not available on " . __PACKAGE__ );
    };
}

1;

__END__

=pod

=head1 SYNOPSIS

    # OO API
    $stream  = $bucket->open_upload_stream("foo.txt");
    $stream->print( $data );
    $stream->close;
    $id = $stream->id;

    # Tied handle API
    $fh = $stream->fh
    print {$fh} $data;
    close $fh;

=head1 DESCRIPTION

This class provides a file abstraction for uploading.  You can stream data
to an object of this class via methods or via a tied-handle interface.

Writes are buffered and sent in chunk-size units.  When C<close> is called,
all data will be flushed to the GridFS Bucket and the newly created file
will be visible.

=head1 CAVEATS

=head2 Character encodings

All the writer methods (e.g. C<print>, C<printf>, etc.) send a binary
representation of the string input provided (or generated in the case of
C<printf>).  Unless you explicitly encode it to bytes, this will be the
B<internal> representation of the string in the Perl interpreter.  If you
have ASCII characters, it will already be bytes.  If you have any
characters above C<0xff>, it will be UTF-8 encoded codepoints.  If you have
characters between C<0x80> and C<0xff> and not higher, you might have
either bytes or UTF-8 internally.

B<You are strongly encouraged to do your own character encoding with
the L<Encode> module or equivalent and upload only bytes to GridFS>.

=cut
