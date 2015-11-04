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

package MongoDB::GridFSBucket;

# ABSTRACT: A file storage utility

use Moo;
use MongoDB::GridFSBucket::DownloadStream;
use MongoDB::_Types qw(
    ReadPreference
    WriteConcern
    BSONCodec
    NonNegNum
);
use Types::Standard qw(
    Int
    Str
    InstanceOf
);
use namespace::clean -except => 'meta';

has database => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::Database'],
    required => 1,
);

=attr bucket_name

The name of the GridFS bucket.  Defaults to 'fs'.

=cut

has bucket_name => (
    is      => 'ro',
    isa     => Str,
    default => sub { 'fs' },
);

=attr chunk_size_bytes

The number of bytes per chunk.  Defaults to 261120 (255kb).

=cut

has chunk_size_bytes => (
    is      => 'ro',
    isa     => NonNegNum,
    default => sub { 255 * 1024 },
);

=attr write_concern

A L<MongoDB::WriteConcern> object.  It may be initialized with a hash
reference that will be coerced into a new MongoDB::WriteConcern object.
By default it will be inherited from a L<MongoDB::Database> object.

=cut

has write_concern => (
    is       => 'ro',
    isa      => WriteConcern,
    required => 1,
    coerce   => WriteConcern->coercion,
);

=attr read_preference

A L<MongoDB::ReadPreference> object.  It may be initialized with a string
corresponding to one of the valid read preference modes or a hash reference
that will be coerced into a new MongoDB::ReadPreference object.
By default it will be inherited from a L<MongoDB::Database> object.

=cut

has read_preference => (
    is       => 'ro',
    isa      => ReadPreference,
    required => 1,
    coerce   => ReadPreference->coercion,
);

=attr bson_codec

An object that provides the C<encode_one> and C<decode_one> methods, such as
from L<MongoDB::BSON>.  It may be initialized with a hash reference that will
be coerced into a new MongoDB::BSON object.  By default it will be inherited
from a L<MongoDB::MongoClient> object.

=cut

has bson_codec => (
    is       => 'ro',
    isa      => BSONCodec,
    coerce   => BSONCodec->coercion,
    required => 1,
);

=attr max_time_ms

Specifies the maximum amount of time in milliseconds that the server should use
for working on a query.

B<Note>: this will only be used for server versions 2.6 or greater, as that
was when the C<$maxTimeMS> meta-operator was introduced.

=cut

has max_time_ms => (
    is       => 'ro',
    isa      => NonNegNum,
    required => 1,
);

=method files

The L<MongoDB::Collection> used to store the files documents for the bucket.
See L<https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst#terms>
for more information.

=cut

has _files => (
    is       => 'lazy',
    isa      => InstanceOf['MongoDB::Collection'],
    reader   => 'files',
    init_arg => undef,
);

sub _build__files {
    my $self = shift;
    my $coll = $self->database->get_collection(
        $self->bucket_name . '.files',
        {
            read_preference => $self->read_preference,
            write_concern   => $self->write_concern,
            max_time_ms     => $self->max_time_ms,
            bson_codec      => $self->bson_codec,
        }
    );
    return $coll;
}

=method chunks

The L<MongoDB::Collection> used to store the chunks documents for the bucket.
See L<https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst#terms>
for more information.

=cut

has _chunks => (
    is       => 'lazy',
    isa      => InstanceOf['MongoDB::Collection'],
    reader   => 'chunks',
    init_arg => undef,
);

sub _build__chunks {
    my $self = shift;
    my $coll = $self->database->get_collection(
        $self->bucket_name . '.chunks',
        {
            read_preference => $self->read_preference,
            write_concern   => $self->write_concern,
            max_time_ms     => $self->max_time_ms,
            # XXX: Generate a new bson codec here to
            # prevent users from changing it?
            bson_codec      => $self->bson_codec,
        }
    );
    return $coll;
}

sub _ensure_indexes {
    my ($self) = @_;

    # ensure the necessary index is present (this may be first usage)
    $self->files->indexes->create_one([ filename => 1, uploadDate => 1 ]);
    $self->chunks->indexes->create_one([ files_id => 1, n => 1 ]);
}

=method

    $bucket->delete($id);

Deletes a file from from the bucket matching C<$id>. throws a
L<MongoDB::GridFSError> if no such file exists.

=cut

sub delete {
    my ($self, $id) = @_;
    my $delete_result = $self->files->delete_one({ _id => $id });
    # This should only ever be 0 or 1, checking for exactly 1 to be thorough
    unless ($delete_result->deleted_count == 1) {
        MongoDB::GridFSError->throw(sprintf(
            'found %d files instead of 1 for id %s',
            $delete_result->deleted_count, $id,
        ));
    }
    $self->chunks->delete_many({ files_id => $id });
    return;
}

=method

    $bucket->find($filter);
    $bucket->find($filter, $options);

Executes a query on the files collection with a
L<filter expression|/Filter expression> and
returns a C<MongoDB::QueryResult> object.

=cut

sub find {
    my ($self, $filter, $options) = @_;
    return $self->files->find($filter, $options)->result;
}

=method drop

    $bucket->drop;

Drops the files and chunks collections for this bucket.

=cut

sub drop {
    my ($self) = @_;
    $self->files->drop;
    $self->chunks->drop;
}

=method download_to_stream

    $bucket->download_to_stream($id, $fh);

Downloads the file matching C<$id> and writes it to the
file handle C<$fh>.

=cut

sub download_to_stream {
    my ($self, $id, $fh) = @_;
    MongoDB::UsageError->throw('No id provided to download_to_stream') unless $id;

    my $file_doc = $self->files->find_one({ _id => $id });
    if (!$file_doc) {
        MongoDB::GridFSError->throw("No file document found for id '$id'");
    }
    return unless $file_doc->{length} > 0;

    my $chunks = $self->chunks->find({ files_id => $id }, { sort => { n => 1 } })->result;
    my $last_chunk_n = int($file_doc->{'length'} / $file_doc->{'chunkSize'});
    for my $n (0..($last_chunk_n)) {
        if (!$chunks->has_next) {
            MongoDB::GridFSError->throw("Missing chunk $n for file with id $id");
        }
        my $chunk = $chunks->next;
        if ( $chunk->{'n'} != $n) {
            MongoDB::GridFSError->throw(sprintf(
                    'Expected chunk %d but got chunk %d',
                    $n, $chunk->{'n'},
            ));
        }
        my $expected_size = $chunk->{'n'} == $last_chunk_n ?
            $file_doc->{'length'} % $file_doc->{'chunkSize'} :
            $file_doc->{'chunkSize'};
        if ( length $chunk->{'data'} != $expected_size ) {
            MongoDB::GridFSError->throw(sprintf(
                "Chunk $n from file with id $id has incorrect size %d, expected %d",
                length $chunk->{'data'}, $expected_size,
            ));
        }
        print $fh $chunk->{data};
    }
    if ( $chunks->has_next ) {
        MongoDB::GridFSError->throw("File with id $id has extra chunks");
    }
    return;
}

=method open_download_stream

    my $stream = $bucket->open_download_stream;

Returns a new L<MongoDB::GridFSBucket::DownloadStream> for this bucket.

=cut

sub open_download_stream {
    my ($self, $id) = @_;
    return unless $id;
    return MongoDB::GridFSBucket::DownloadStream->new({
        id    => $id,
        bucket => $self,
    });
}

1;
