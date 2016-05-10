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

# ABSTRACT: A file storage abstraction

use version;
our $VERSION = 'v1.5.0';

use Moo;
use MongoDB::GridFSBucket::DownloadStream;
use MongoDB::GridFSBucket::UploadStream;
use MongoDB::_Types qw(
  ReadPreference
  WriteConcern
  ReadConcern
  BSONCodec
  NonNegNum
);
use Scalar::Util qw/reftype/;
use Types::Standard qw(
  Bool
  Int
  Str
  InstanceOf
);
use namespace::clean -except => 'meta';

=attr database

The L<MongoDB::Database> containing the GridFS bucket collections.

=cut

has database => (
    is       => 'ro',
    isa      => InstanceOf ['MongoDB::Database'],
    required => 1,
);

=attr bucket_name

The name of the GridFS bucket.  Defaults to 'fs'.  The underlying
collections that are used to implement a GridFS bucket get this string as a
prefix (e.g "fs.chunks").

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

=attr read_concern

A L<MongoDB::ReadConcern> object.  May be initialized with a hash
reference or a string that will be coerced into the level of read
concern.

By default it will be inherited from a L<MongoDB::Database> object.

=cut

has read_concern => (
    is       => 'ro',
    isa      => ReadConcern,
    required => 1,
    coerce   => ReadConcern->coercion,
);

=attr read_preference

A L<MongoDB::ReadPreference> object.  It may be initialized with a string
corresponding to one of the valid read preference modes or a hash reference
that will be coerced into a new MongoDB::ReadPreference object.
By default it will be inherited from a L<MongoDB::Database> object.

B<Note:> Because many GridFS operations require multiple independent reads from
separate collections, use with secondaries is B<strongly discouraged> because
reads could go to different secondaries, resulting in inconsistent data
if all file and chunk documents have not replicated to all secondaries.

=cut

has read_preference => (
    is       => 'ro',
    isa      => ReadPreference,
    required => 1,
    coerce   => ReadPreference->coercion,
);

=attr bson_codec

An object that provides the C<encode_one> and C<decode_one> methods, such
as from L<MongoDB::BSON>.  It may be initialized with a hash reference that
will be coerced into a new MongoDB::BSON object.  By default it will be
inherited from a L<MongoDB::Database> object.

=cut

has bson_codec => (
    is       => 'ro',
    isa      => BSONCodec,
    coerce   => BSONCodec->coercion,
    required => 1,
);

=attr max_time_ms

Specifies the maximum amount of time in milliseconds that the server should
use for working on a query.  By default it will be inherited from a
L<MongoDB::Database> object.

B<Note>: this will only be used for server versions 2.6 or greater, as that
was when the C<$maxTimeMS> meta-operator was introduced.

=cut

has max_time_ms => (
    is       => 'ro',
    isa      => NonNegNum,
    required => 1,
);

# determines whether or not to attempt index creation
has _tried_indexing => (
    is => 'rwp',
    isa => Bool,
);

has _files => (
    is       => 'lazy',
    isa      => InstanceOf ['MongoDB::Collection'],
    init_arg => undef,
);

sub _build__files {
    my $self = shift;
    my $coll = $self->database->get_collection(
        $self->bucket_name . '.files',
        {
            read_preference => $self->read_preference,
            write_concern   => $self->write_concern,
            read_concern    => $self->read_concern,
            max_time_ms     => $self->max_time_ms,
            bson_codec      => $self->bson_codec,
        }
    );
    return $coll;
}

has _chunks => (
    is       => 'lazy',
    isa      => InstanceOf ['MongoDB::Collection'],
    init_arg => undef,
);

sub _build__chunks {
    my $self = shift;
    my $coll = $self->database->get_collection(
        $self->bucket_name . '.chunks',
        {
            read_preference => $self->read_preference,
            write_concern   => $self->write_concern,
            read_concern    => $self->read_concern,
            max_time_ms     => $self->max_time_ms,
            # XXX: Generate a new bson codec here to
            # prevent users from changing it?
            bson_codec => $self->bson_codec,
        }
    );
    return $coll;
}

# index operations need primary server, regardless of bucket read prefs
sub _create_indexes {
    my ($self) = @_;
    $self->_set__tried_indexing(1);

    my $pf = $self->_files->clone( read_preference => 'primary' );

    return if $pf->count > 0;

    my $pfi = $pf->indexes;
    my $pci = $self->_chunks->clone( read_preference => 'primary' )->indexes;

    if ( !grep { $_->{name} eq 'filename_1_uploadDate_1' } $pfi->list->all ) {
        $pfi->create_one( [ filename => 1, uploadDate => 1 ], { unique => 1 } );
    }

    if ( !grep { $_->{name} eq 'files_id_1_n_1' } $pci->list->all ) {
        $pci->create_one( [ files_id => 1, n => 1 ], { unique => 1 } );
    }

    return;
}

=method find

    $result = $bucket->find($filter);
    $result = $bucket->find($filter, $options);

    $file_doc = $result->next;

Executes a query on the file documents collection with a
L<filter expression|MongoDB::Collection/Filter expression> and
returns a L<MongoDB::QueryResult> object.  It takes an optional hashref
of options identical to L<MongoDB::Collection/find>.

=cut

sub find {
    my ( $self, $filter, $options ) = @_;
    return $self->_files->find( $filter, $options )->result;
}

=method find_one

    $file_doc = $bucket->find_one($filter, $projection);
    $file_doc = $bucket->find_one($filter, $projection, $options);

Executes a query on the file documents collection with a
L<filter expression|MongoDB::Collection/Filter expression> and
returns the first document found, or C<undef> if no document is found.

See L<MongoDB::Collection/find_one> for details about the
C<$projection> and optional C<$options> fields.

=cut

sub find_one {
    my ( $self, $filter, $projection, $options ) = @_;
    return $self->_files->find_one( $filter, $projection, $options );
}

=method find_id

    $file_doc = $bucket->find_id( $id );
    $file_doc = $bucket->find_id( $id, $projection );
    $file_doc = $bucket->find_id( $id, $projection, $options );

Executes a query with a L<filter expression|/Filter expression> of
C<< { _id => $id } >> and returns a single document or C<undef> if no document
is found.

See L<MongoDB::Collection/find_one> for details about the
C<$projection> and optional C<$options> fields.

=cut

sub find_id {
    my ( $self, $id, $projection, $options ) = @_;
    return $self->_files->find_id( $id, $projection, $options );
}

=method open_download_stream

    $stream = $bucket->open_download_stream($id);
    $line = $stream->readline;

Returns a new L<MongoDB::GridFSBucket::DownloadStream> that can be used to
download the file with the file document C<_id> matching C<$id>.  This
throws a L<MongoDB::GridFSError> if no such file exists.

=cut

sub open_download_stream {
    my ( $self, $id ) = @_;
    MongoDB::UsageError->throw('No id provided to open_download_stream') unless $id;
    my $file_doc = $self->_files->find_id($id);
    MongoDB::GridFSError->throw("FileNotFound: no file found for id '$id'")
      unless $file_doc;
    my $result =
        $file_doc->{'length'} > 0
      ? $self->_chunks->find( { files_id => $id }, { sort => { n => 1 } } )->result
      : undef;
    return MongoDB::GridFSBucket::DownloadStream->new(
        {
            id       => $id,
            file_doc => $file_doc,
            _result  => $result,
        }
    );
}

=method open_upload_stream

    $stream = $bucket->open_upload_stream($filename);
    $stream = $bucket->open_upload_stream($filename, $options);

    $stream->print('data');
    $stream->close;
    $file_id = $stream->id

Returns a new L<MongoDB::GridFSBucket::UploadStream> that can be used
to upload a new file to a GridFS bucket.

This method requires a filename to store in the C<filename> field of the
file document.  B<Note>: the filename is an arbitrary string; the method
does not read from this filename locally.

You can provide an optional hash reference of options that are passed to the
L<MongoDB::GridFSBucket::UploadStream> constructor:

=for :list
* C<chunk_size_bytes> – the number of bytes per chunk.  Defaults to the
  C<chunk_size_bytes> of the bucket object.
* C<metadata> – a hash reference for storing arbitrary metadata about the
  file.

=cut

sub open_upload_stream {
    my ( $self, $filename, $options ) = @_;
    MongoDB::UsageError->throw('No filename provided to open_upload_stream')
      unless defined $filename && length $filename;

    $self->_create_indexes unless $self->_tried_indexing;

    return MongoDB::GridFSBucket::UploadStream->new(
        {
            chunk_size_bytes => $self->chunk_size_bytes,
            ( $options ? %$options : () ),
            _bucket  => $self,
            filename => "$filename", # stringify path objects
        }
    );
}

=method download_to_stream

    $bucket->download_to_stream($id, $out_fh);

Downloads the file matching C<$id> and writes it to the file handle C<$out_fh>.
This throws a L<MongoDB::GridFSError> if no such file exists.

=cut

sub download_to_stream {
    my ( $self, $id, $target_fh ) = @_;
    MongoDB::UsageError->throw('No id provided to download_to_stream')
      unless defined $id;
    MongoDB::UsageError->throw('No handle provided to download_to_stream')
      unless defined $target_fh;
    MongoDB::UsageError->throw(
        'Invalid handle $target_fh provided to download_to_stream')
      unless reftype $target_fh eq 'GLOB';

    my $download_stream = $self->open_download_stream($id);
    my $csb             = $download_stream->file_doc->{chunkSize};
    my $buffer;
    while ( $download_stream->read( $buffer, $csb ) ) {
        print {$target_fh} $buffer;
    }
    $download_stream->close;
    return;
}

=method upload_from_stream

    $file_id = $bucket->upload_from_stream($filename, $in_fh);
    $file_id = $bucket->upload_from_stream($filename, $in_fh, $options);

Reads from a filehandle and uploads its contents to GridFS.  It returns the
C<_id> field stored in the file document.

This method requires a filename to store in the C<filename> field of the
file document.  B<Note>: the filename is an arbitrary string; the method
does not read from this filename locally.

You can provide an optional hash reference of options, just like
L</open_upload_stream>.

=cut

sub upload_from_stream {
    my ( $self, $filename, $source_fh, $options ) = @_;
    MongoDB::UsageError->throw('No filename provided to upload_from_stream')
      unless defined $filename && length $filename;
    MongoDB::UsageError->throw('No handle provided to upload_from_stream')
      unless defined $source_fh;
    MongoDB::UsageError->throw(
        'Invalid handle $source_fh provided to upload_from_stream')
      unless reftype $source_fh eq 'GLOB';

    my $upload_stream = $self->open_upload_stream( $filename, $options );
    my $csb = $upload_stream->chunk_size_bytes;
    my $buffer;
    while ( read $source_fh, $buffer, $csb ) {
        $upload_stream->print($buffer);
    }
    $upload_stream->close;
    return $upload_stream->id;
}

=method delete

    $bucket->delete($id);

Deletes the file matching C<$id> from the bucket.
This throws a L<MongoDB::GridFSError> if no such file exists.

=cut

sub delete {
    my ( $self, $id ) = @_;

    $self->_create_indexes unless $self->_tried_indexing;

    my $delete_result = $self->_files->delete_one( { _id => $id } );
    # This should only ever be 0 or 1, checking for exactly 1 to be thorough
    unless ( $delete_result->deleted_count == 1 ) {
        MongoDB::GridFSError->throw("FileNotFound: no file found for id $id");
    }
    $self->_chunks->delete_many( { files_id => $id } );
    return;
}

=method drop

    $bucket->drop;

Drops the underlying files documents and chunks collections for this bucket.

=cut

sub drop {
    my ($self) = @_;
    $self->_files->drop;
    $self->_chunks->drop;
}

1;

__END__

=pod

=head1 SYNOPSIS

    $bucket = $database->gfs;

    # upload a file
    $stream  = $bucket->open_upload_stream("foo.txt");
    $stream->print( $data );
    $stream->close;

    # find and download a file
    $result  = $bucket-find({filename => "foo.txt"});
    $file_id = $result->next->{_id};
    $stream  = $bucket->open_download_stream($file_id)
    $data    = do { local $/; $stream->readline() };

=head1 DESCRIPTION

This class models a GridFS file store in a MongoDB database and provides an
API for interacting with it.

Generally, you never construct one of these directly with C<new>.  Instead,
you call C<gfs> (short for C<get_gridfsbucket>) on a L<MongoDB::Database>
object.

=head1 USAGE

=head2 Data model

A GridFS file is represented in MongoDB as a "file document" with
information like the file's name, length, MD5 hash, and any user-supplied
metadata.  The actual contents are stored as a number of "chunks" of binary
data.  (Think of the file document as a directory entry and the chunks like
blocks on disk.)

Valid file documents typically include the following fields:

=for :list
* _id – a unique ID for this document, typically type BSON ObjectId. Legacy
  GridFS documents may store this value as a different type. New files must
  be stored using an ObjectId.
* length – the length of this stored file, in bytes
* chunkSize – the size, in bytes, of each full data chunk of this file. This
  value is configurable per file.
* uploadDate – the date and time this file was added to GridFS, stored as
  a BSON datetime value.
* md5 – a hash of the contents of the stored file
* filename – the name of this stored file; this does not need to be unique
* metadata – any additional application data the user wishes to store
  (optional)
* contentType – DEPRECATED (store this in C<metadata> if you need it)
  (optional)
* aliases – DEPRECATED (store this in C<metadata> if you need it)
  (optional)

The C<find> method searches file documents using these fields.  Given the
C<_id> from a document, a file can be downloaded using the download
methods.

=head2 API Overview

In addition to general methods like C<find>, C<delete> and C<drop>, there
are two ways to go about uploading and downloading:

=for :list
* filehandle-like: you get an object that you can read/write from similar
  to a filehandle.  You can even get a tied filehandle that you can
  hand off to other code that requires an actual Perl handle.
* streaming: you provide a file handle to read from (upload) or print
  to (download) and data is streamed to (upload) or from (download)
  GridFS until EOF.

=head2 Error handling

Unless otherwise explictly documented, all methods throw exceptions if
an error occurs.  The error types are documented in L<MongoDB::Error>.

=head1 SEE ALSO

Core documentation on GridFS: L<http://dochub.mongodb.org/core/gridfs>.

