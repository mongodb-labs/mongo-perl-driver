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
use MongoDB::WriteConcern;
use MongoDB::GridFSBucket::DownloadStream;
use MongoDB::_Types qw(
    ReadPreference
    WriteConcern
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
    isa     => Int,
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

has files => (
    is => 'lazy',
    isa => InstanceOf['MongoDB::Collection'],
);

sub _build_files {
    my $self = shift;
    my $coll = $self->database->get_collection(
        $self->bucket_name . '.files',
        {
            read_preference => $self->read_preference,
            write_concern   => $self->write_concern,
            # max_time_ms     => $self->max_time_ms,
            # bson_codec      => $self->bson_codec,
        }
    );
    return $coll;
}

has chunks => (
    is => 'lazy',
    isa => InstanceOf['MongoDB::Collection'],
);

sub _build_chunks {
    my $self = shift;
    my $coll = $self->database->get_collection(
        $self->bucket_name . '.chunks',
        {
            read_preference => $self->read_preference,
            write_concern   => $self->write_concern,
            # max_time_ms     => $self->max_time_ms,
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

sub find {
    my ($self, $filter, $options) = @_;
    return $self->files->find($filter, $options)->result;
}

sub drop {
    my ($self) = @_;
    $self->files->drop;
    $self->chunks->drop;
}

sub download_to_stream {
    my ($self, $id, $fh) = @_;

    my $file_doc = $self->files->find_one({ _id => $id });
    return unless $file_doc && $file_doc->{length} > 0;

    my $chunks = $self->chunks->find({ files_id => $id }, { sort => { n => 1 } });
    while ($chunks->has_next) {
        my $chunk = $chunks->next;
        print $fh $chunk->{data};
    }
    return;
}

sub open_download_stream {
    my ($self, $id) = @_;
    return unless $id;
    return MongoDB::GridFSBucket::DownloadStream->new({
        _id    => $id,
        bucket => $self,
    });
}

1;
