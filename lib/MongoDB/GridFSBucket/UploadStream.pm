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
    Int
    HashRef
    ArrayRef
    InstanceOf
);
use MongoDB::_Types qw(
    NonNegNum
);
use Digest::MD5;
use namespace::clean -except => 'meta';

=attr chunk_size_bytes

The number of bytes per chunk.  Defaults to 261120 (255kb).

=cut

has chunk_size_bytes => (
    is      => 'ro',
    isa     => Int,
    default => sub { 255 * 1024 },
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
    isa     => Int,
    default => sub { 0 },
);

sub _build_id {
    return MongoDB::OID->new;
}

has _buffer => (
    is      => 'rw',
    isa     => Str,
    default => sub { '' },
);

has _length => (
    is      => 'rw',
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
    isa => Int,
);

sub _build__chunk_buffer_length {
    my ($self) = @_;
    # FIXME: compute this based on the size of the documents?
    return 10 * $self->bucket->chunk_size_bytes;
}

has _current_chunk_n => (
    is      => 'rw',
    isa     => NonNegNum,
    default => sub { 0 },
);

sub _flush_chunks {
    my ($self, $all) = @_;
    my @chunks = ();
    my $data;
    while ( ($data = substr $self->{_buffer}, 0, $self->bucket->chunk_size_bytes, '') ) {
        if ( length $data < $self->chunk_size_bytes && !$all ) {
            $self->_buffer($data);
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

sub print {
    my $self = shift;
    return if $self->closed;
    my $fsep = $, ? $, : '';
    my $osep = $\ ? $\ : '';
    my $output = join($fsep, @_) . $osep;
    $self->_write_data($output);
}

sub printf {
    my $self = shift;
    my $savedos = $\;
    $\ = undef;
    $self->print(@_);
    $\ = $savedos;
}

sub write {
    ...
}

sub close {
    my ($self) = @_;
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

*PRINT = \&print;
*PRINTF = \&printf;
*WRITE = \&write;
*CLOSE = \&close;

1;
