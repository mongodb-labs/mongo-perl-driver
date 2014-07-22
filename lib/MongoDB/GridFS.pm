#
#  Copyright 2009-2013 MongoDB, Inc.
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

package MongoDB::GridFS;


# ABSTRACT: A file storage utility

use version;
our $VERSION = 'v0.704.2.1';

use MongoDB::GridFS::File;
use DateTime 0.78; # drops dependency on bug-prone Math::Round
use Digest::MD5;
use Moose;
use namespace::clean -except => 'meta';
use Carp 'carp';

=head1 NAME

MongoDB::GridFS - A file storage utility

=head1 SYNOPSIS

    use MongoDB::GridFS;

    my $grid = $database->get_gridfs;
    my $fh = IO::File->new("myfile", "r");
    $grid->insert($fh, {"filename" => "mydbfile"});

There are two interfaces for GridFS: a file-system/collection-like interface
(insert, remove, drop, find_one) and a more general interface
(get, put, delete).  Their functionality is the almost identical (get, put and
delete are always safe ops, insert, remove, and find_one are optionally safe),
using one over the other is a matter of preference.

=head1 SEE ALSO

Core documentation on GridFS: L<http://dochub.mongodb.org/core/gridfs>.

=head1 ATTRIBUTES

=head2 chunk_size

The number of bytes per chunk.  Defaults to 261120 (255kb).

=cut

$MongoDB::GridFS::chunk_size = 261120;

has _database => (
    is       => 'ro',
    isa      => 'MongoDB::Database',
    required => 1,
);

=head2 prefix

The prefix used for the collections.  Defaults to "fs".

=cut

has prefix => (
    is      => 'ro',
    isa     => 'Str',
    default => 'fs'
);

=head2 files

Collection in which file metadata is stored.  Each document contains md5 and
length fields, plus user-defined metadata (and an _id).

=cut

has files => (
    is => 'ro',
    isa => 'MongoDB::Collection',
    lazy_build => 1
);

sub _build_files {
    my $self = shift;
    my $coll = $self->_database->get_collection($self->prefix . '.files');
    return $coll;
}

=head2 chunks

Actual content of the files stored.  Each chunk contains up to 4Mb of data, as
well as a number (its order within the file) and a files_id (the _id of the file
in the files collection it belongs to).

=cut

has chunks => (
    is => 'ro',
    isa => 'MongoDB::Collection',
    lazy_build => 1
);

sub _build_chunks {
    my $self = shift;
    my $coll = $self->_database->get_collection($self->prefix . '.chunks');
    return $coll;
}

# This checks if the required indexes for GridFS exist in for the current database.
# If they are not found, they will be created.
sub BUILD {
    my ($self) = @_;
   
    # check for the required indexes in the system.indexes collection
    my $count = $self->_database->get_collection('system.indexes')->count({key=>{filename => 1}});
    $count   += $self->_database->get_collection('system.indexes')->count({key=>{files_id => 1, n => 1}});
    
    # if we don't have the required indexes, create them now.
    if ($count < 2){
       $self->_ensure_indexes();
    }
}


sub _ensure_indexes {
    my ($self) = @_;

    # ensure the necessary index is present (this may be first usage)
    $self->files->ensure_index(Tie::IxHash->new(filename => 1), {write_concern => {w => 1}});
    $self->chunks->ensure_index(Tie::IxHash->new(files_id => 1, n => 1), {write_concern => {w => 1}, unique => 1});
}

=head1 METHODS

=head2 get($id)

    my $file = $grid->get($id);

Get a file from GridFS based on its _id.  Returns a L<MongoDB::GridFS::File>.

To retrieve a file based on metadata like C<filename>, use the L</find_one>
method instead.

=cut

sub get {
    my ($self, $id) = @_;

    return $self->find_one({_id => $id});
}

=head2 put($fh, $metadata)

    my $id = $grid->put($fh, {filename => "pic.jpg"});

Inserts a file into GridFS, adding a L<MongoDB::OID> as the _id field if the
field is not already defined.  This is a wrapper for C<MongoDB::GridFS::insert>,
see that method below for more information.

To use a filename as the _id for subsequent C<get> calls, you must set _id
explicitly:

    $grid->put($fh, {_id => "pic.jpg", filename => "pic.jpg"});
    my $file = $grid->get("pic.jpg");

Returns the _id field.

=cut

sub put {
    my ($self, $fh, $metadata) = @_;

    return $self->insert($fh, $metadata, {write_concern => {w => 1}});
}

=head2 delete($id)

    $grid->delete($id)

Removes the file with the given _id.  Will die if the remove is unsuccessful.
Does not return anything on success.

=cut

sub delete {
    my ($self, $id) = @_;

    $self->remove({_id => $id}, {write_concern => {w => 1}});
}

=head2 find_one ($criteria?, $fields?)

    my $file = $grid->find_one({"filename" => "foo.txt"});

Returns a matching MongoDB::GridFS::File or undef.

=cut

sub find_one {
    my ($self, $criteria, $fields) = @_;

    my $file = $self->files->find_one($criteria, $fields);
    return undef unless $file;
    return MongoDB::GridFS::File->new({_grid => $self,info => $file});
}

=head2 remove ($criteria?, $options?)

    $grid->remove({"filename" => "foo.txt"});

Cleanly removes files from the database.  C<$options> is a hash of options for
the remove.  Possible options are:

=over 4

=item just_one
If true, only one file matching the criteria will be removed.

=back

This method doesn't return anything.

=cut

sub remove {
    my ($self, $criteria, $options) = @_;

    my $just_one = 0;
    my $wc = $self->_database->_client->_write_concern;

    if (defined $options) {
        if (ref $options eq 'HASH') {
            $just_one = $options->{just_one} && 1;

            my $new_wc = $options->{write_concern} || $options->{safe};

            # XXX Remove ability to use safe in next MAJOR version.
            if ($options->{safe}) {
                carp "safe is deprecated, use write concern instead.";
                $new_wc = {w => 1};
            }

            $wc = $new_wc || $wc;
        }
        elsif ($options) {
            $just_one = $options && 1;
        }
    }

    if ($just_one) {
        my $meta = $self->files->find_one($criteria);
        $self->chunks->remove({"files_id" => $meta->{'_id'}}, {write_concern => $wc});
        $self->files->remove({"_id" => $meta->{'_id'}}, {write_concern => $wc});
    }
    else {
        my $cursor = $self->files->query($criteria);
        while (my $meta = $cursor->next) {
            $self->chunks->remove({"files_id" => $meta->{'_id'}}, {write_concern => $wc});
        }
        $self->files->remove($criteria, {write_concern => $wc});
    }
}


=head2 insert ($fh, $metadata?, $options?)

    my $id = $gridfs->insert($fh, {"content-type" => "text/html"});

Reads from a file handle into the database.  Saves the file with the given
metadata.  The file handle must be readable.  C<$options> can be
C<{"write_concern" => {"w" => 1}}>, which will do safe inserts and check the MD5 hash
calculated by the database against an MD5 hash calculated by the local
filesystem.  If the two hashes do not match, then the chunks already inserted
will be removed and the program will die.

Because C<MongoDB::GridFS::insert> takes a file handle, it can be used to insert
very long strings into the database (as well as files).  C<$fh> must be a
FileHandle (not just the native file handle type), so you can insert a string
with:

    # open the string like a file
    my $basic_fh;
    open($basic_fh, '<', \$very_long_string);

    # turn the file handle into a FileHandle
    my $fh = FileHandle->new;
    $fh->fdopen($basic_fh, 'r');

    $gridfs->insert($fh);

=cut

sub insert {
    my ($self, $fh, $metadata, $options) = @_;
    $options ||= {};

    confess "not a file handle" unless $fh;
    $metadata = {} unless $metadata && ref $metadata eq 'HASH';

    my $start_pos = $fh->getpos();

    my $id;
    if (exists $metadata->{"_id"}) {
        $id = $metadata->{"_id"};
    }
    else {
        $id = MongoDB::OID->new;
    }

    my $n = 0;
    my $length = 0;
    while ((my $len = $fh->read(my $data, $MongoDB::GridFS::chunk_size)) != 0) {
        $self->chunks->insert({"files_id" => $id,
                               "n"        => $n,
                               "data"     => bless(\$data)}, $options);
        $n++;
        $length += $len;
    }
    $fh->setpos($start_pos);

    my %copy = %{$metadata};
    # compare the md5 hashes
    if ($options->{write_concern} or $options->{safe}) {
        carp "safe is deprecated, use write concern instead." if $options->{safe};

        # get an md5 hash for the file. set the retry flag to 'true' incase the 
        # database, collection, or indexes are missing. That way we can recreate them 
        # retry the md5 calc.
        my $result = $self->_calc_md5($id, $self->prefix, 1);
        $copy{"md5"} = $result->{"md5"};

        my $md5 = Digest::MD5->new;
        $md5->addfile($fh);
        my $digest = $md5->hexdigest;
        if ($digest ne $result->{md5}) {
            # cleanup and die
            $self->chunks->remove({files_id => $id});
            die "md5 hashes don't match: database got $result->{md5}, fs got $digest";
        }
    }

    $copy{"_id"} = $id;
    $copy{"chunkSize"} = $MongoDB::GridFS::chunk_size;
    $copy{"uploadDate"} = DateTime->now;
    $copy{"length"} = $length;
    return $self->files->insert(\%copy, $options);
}

# Calculates the md5 of the file on the server
# $id    : reference to the object we want to hash
# $root  : the namespace the file resides in
# $retry : a flag which controls whether or not to retry the md5 calc. 
#         (which is currently only if we are missing our indexes)
sub _calc_md5 {
    my ($self, $id, $root, $retry) = @_;
   
    # Try to get an md5 hash for the file
    my $result = $self->_database->run_command(["filemd5", $id, "root" => $self->prefix]);
    
    # If we didn't get a hash back, it means something is wrong (probably to do with gridfs's 
    # indexes because its currently the only error that is thrown from the md5 class)
    if (ref($result) ne 'HASH') {
        # Yep, indexes are missing. If we have the $retry flag, lets create them calc the md5 again
        # but we wont pass set the $retry flag again. We don't want an infinite loop for any reason. 
        if ($retry == 1 && $result eq 'need an index on { files_id : 1 , n : 1 }'){
            $self->_ensure_indexes();
            $result = $self->_calc_md5($id, $root, 0);
        }
        # Well, something bad is happening, so lets clean up and die. 
        else{
            $self->chunks->remove({files_id => $id});
            die "recieve an unexpected error from the server: $result";
        }
    }
    
    return $result;
}


=head2 drop

    @files = $grid->drop;

Removes all files' metadata and contents.

=cut

sub drop {
    my ($self) = @_;

    $self->files->drop;
    $self->chunks->drop;
    $self->_ensure_indexes;
}

=head2 all

    @files = $grid->all;

Returns a list of the files in the database.

=cut

sub all {
    my ($self) = @_;
    my @ret;

    my $cursor = $self->files->query;
    while (my $meta = $cursor->next) {
        push @ret, MongoDB::GridFS::File->new(
            _grid => $self,
            info => $meta);
    }
    return @ret;
}

__PACKAGE__->meta->make_immutable;

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
