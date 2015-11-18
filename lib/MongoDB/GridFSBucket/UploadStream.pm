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
#

package MongoDB::GridFSBucket::UploadStream;

use Moo;
use MongoDB::OID;
use MongoDB::BSON::Binary;
use Types::Standard qw(
    Str
    Bool
    HashRef
    ArrayRef
    InstanceOf
);
use MongoDB::_Types qw(
    NonNegNum
);
use MongoDB::_Constants;
use Digest::MD5;
use bytes;
use namespace::clean -except => 'meta';

=attr chunk_size_bytes

The number of bytes per chunk.  Defaults to 261120 (255kb).

=cut

has chunk_size_bytes => (
    is      => 'ro',
    isa     => NonNegNum,
    default => 255 * 1024,
);

has metadata => (
    is  => 'ro',
    isa => HashRef,
);

has filename => (
    is  => 'ro',
    isa => Str,
);

has content_type => (
    is  => 'ro',
    isa => Str,
);

has aliases => (
    is  => 'ro',
    isa => ArrayRef[Str],
);

has bucket => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::GridFSBucket'],
    required => 1,
);

has id => (
    is       => 'lazy',
    isa      => InstanceOf['MongoDB::OID'],
);

has closed => (
    is      => 'ro',
    isa     => Bool,
    default => 0,
);

sub _build_id {
    return MongoDB::OID->new;
}

has _buffer => (
    is      => 'rwp',
    isa     => Str,
    default => '',
);

has _length => (
    is      => 'rwp',
    isa     => NonNegNum,
    default => sub { 0 },
);

has _md5 => (
    is => 'lazy',
    isa => InstanceOf['Digest::MD5'],
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
    default => sub { 0 },
);

=method fh

    my $fh = $uploadstream->fh;
    print $fh, 'test data...';
    close $fh

Returns a new file handle tied to this instance of UploadStream that can be operated on with the functions C<print>, C<printf>, C<syswrite>, and C<close>.

Important notes:

Allowing one of these tied filehandles to fall out of scope will NOT cause close
to be called. This is due to the way tied file handles are implemented in Perl.
For close to be called implicitly, all tied filehandles and the original object
must go out of scope.

Each file handle retrieved this way is tied back to the same object, so calling
close on multiple tied file handles and/or the original object will have the
same effect as calling close on the original object multiple times.

=cut

sub fh {
    my ($self) = @_;
    my $fh = IO::Handle->new();
    tie *$fh, 'MongoDB::GridFSBucket::UploadStream', $self;
    return $fh;
}

sub _flush_chunks {
    my ($self, $all) = @_;
    my @chunks = ();
    my $data;
    while ( ($data = substr $self->_buffer, 0, $self->chunk_size_bytes, '') ) {
        if ( length $data < $self->chunk_size_bytes && !$all ) {
            $self->_set__buffer($data);
            last;
        }

        push @chunks, {
            files_id => $self->id,
            n        => $self->_current_chunk_n,
            data     => MongoDB::BSON::Binary->new({ data => $data }),
        };
        $self->{_current_chunk_n} += 1;
    }
    $self->bucket->chunks->insert_many(\@chunks) unless scalar(@chunks) < 1;
}

sub _write_data {
    my ($self, $data) = @_;
    $self->{_buffer} .= $data;
    $self->{_length} += length $data;
    $self->_md5->add($data);
    $self->_flush_chunks if length $self->_buffer >= $self->_chunk_buffer_length;
}

sub abort {
    my ($self) = @_;

    $self->bucket->files->delete_many({ files_id => $self->id });
    $self->closed(1);
}

=method

    $uploadstream->print('my data...');
    $uploadstream->print('data', 'more data', 'still more data');

Prints a string or a list of strings to the GridFS file.
See the documentation for L<print> for more details

=cut

sub print {
    my $self = shift;
    return if $self->closed;
    my $fsep = $, ? $, : '';
    my $osep = $\ ? $\ : '';
    my $output = join($fsep, @_) . $osep;
    $self->_write_data($output);
    return 1;
}

=method

    $uploadstream->printf('%s: %d', 'the meaning of life, the universe, and everything', 42)

Equivalent to C<$uploadstream->print(sprintf(FORMAT, LIST))>, except that C<$\> is not appended.
See the L<printf> documentation for more details.

=cut

sub printf {
    my $self = shift;
    my $format = shift;
    my $savedos = $\;
    $\ = undef;
    $self->print(sprintf($format, @_));
    $\ = $savedos;
}

=method

    $uploadstream->write(SCALAR, LENGTH, OFFSET);

Attempts to write C<LENGTH> bytes of data from variable C<SCALAR> to the GridFS file.
If C<LENGTH> is not specified, writes whole C<SCALAR>.
See L<write> for more details on how to use C<LENGTH> and C<OFFSET>.

=cut

sub write {
    my($self, $buff, $len, $offset) = @_;
    my $bufflen = length $buff;

    $len = $bufflen unless defined $len;
    if ( $len < 0 ) {
        MongoDB::UsageError->throw('Negative length passed to MongoDB::GridFSBucket::DownloadStream->read')
    };

    $offset ||= 0;
    $offset = max(0, $bufflen) if $offset < 0;

    my $savedos = $\;
    $\ = undef;
    $self->print(substr($buff, $offset, $len));
    $\ = $savedos;
}

=method

    $uploadstream->close;

Closes the stream and flushes any remaining data to the database. Once this is
done a document is created in the C<files> collection, making the uploaded file
visible in the GridFS bucket.

Important Notes:

Calling close will also cause any tied file handles created for this stream to
also close.

C<close> will be automatically called when a stream is garbage collected. When
called this way, any errors thrown will not halt execution.

=cut

sub close {
    my ($self) = @_;
    return if $self->closed;
    $self->_flush_chunks(1);
    my $filedoc = {
        _id         => $self->id,
        length      => $self->_length,
        chunkSize   => $self->chunk_size_bytes,
        uploadDate  => DateTime->now,
        md5         => $self->_md5->hexdigest,
        filename    => $self->filename,
    };
    $filedoc->{'contentType'} = $self->content_type if $self->content_type;
    $filedoc->{'metadata'} = $self->metadata if $self->metadata;
    $filedoc->{'aliases'} = $self->aliases if $self->aliases;
    $self->bucket->files->insert_one($filedoc);
    $self->{closed} = 1;
}

sub DEMOLISH {
    my ($self) = @_;
    $self->close unless $self->closed;
}

sub TIEHANDLE {
    my ($class, $self) = @_;
    return $self;
}

*PRINT = \&print;
*PRINTF = \&printf;
*WRITE = \&write;
*CLOSE = \&close;

1;
