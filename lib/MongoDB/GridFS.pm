#
#  Copyright 2009 10gen, Inc.
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
our $VERSION = '0.22';

# ABSTRACT: A file storage utility

use Any::Moose;
use MongoDB::GridFS::File;

=head1 SYNOPSIS

    use MongoDB::GridFS;

    my $grid = $database->get_gridfs;
    my $fh = IO::File->new("myfile", "r");
    $grid->insert($fh, {"filename" => "mydbfile"});

=cut

$MongoDB::GridFS::chunk_size = 1048576;

has _database => (
    is       => 'ro',
    isa      => 'MongoDB::Database',
    required => 1,
);

=attr files

Collection in which file metadata is stored.  Each
document contains md5 and length fields, plus
user-defined metadata (and an _id). 

=cut

has files => (
    is => 'ro',
    isa => 'MongoDB::Collection',
    required => 1,
);

=attr chunks

Actual content of the files stored.  Each chunk contains
up to 4Mb of data, as well as a number (its order within 
the file) and a files_id (the _id of the file in the 
files collection it belongs to).  

=cut

has chunks => (
    is => 'ro',
    isa => 'MongoDB::Collection',
    required => 1,
);

=method find_one ($criteria)

    my $file = $grid->find_one({"filename" => "foo.txt"});

Returns a matching MongoDB::GridFS::File or undef.

=cut

sub find_one {
    #TODO: add fields
    my ($self, $criteria) = @_;

    my $file = $self->files->find_one($criteria);
    return undef unless $file;
    return MongoDB::GridFS::File->new({_grid => $self,info => $file});
}

=method remove ($criteria)

    $grid->remove({"filename" => "foo.txt"});

Cleanly removes a file from the database.

=cut

sub remove {
    #TODO: add just_one
    my ($self, $criteria) = @_;

    my $cursor = $self->files->query($criteria);
    while (my $meta = $cursor->next) {
        $self->chunks->remove({"files_id" => $meta->{'_id'}});
    }
    $self->files->remove($criteria);
}


=method insert ($fh, $metadata)

    my $id = $gridfs->insert($fh, {"content-type" => "text/html"});

Reads from a file handle into the database.  Saves the file 
with the given metadata.  The file handle must be readable.

=cut

sub insert {
    my $self = shift;
    my $fh = shift;
    my $metadata = shift;

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
                               "n" => $n,
                               "data" => bless(\$data)});
        $n++;
        $length += $len;
    }
    $fh->setpos($start_pos);

    # get an md5 hash for the file
    my $result = $self->_database->run_command({"filemd5", $id, 
                                                "root" => $self->files->full_name});

    my %copy = %{$metadata};
    $copy{"_id"} = $id;
    $copy{"md5"} = $result->{"md5"};
    $copy{"length"} = $length;
    return $self->files->insert(\%copy);
}

=method drop

    @files = $grid->drop;

Removes all files' metadata and contents.

=cut

sub drop {
    my ($self) = @_;

    $self->files->drop;
    $self->chunks->drop;
}

=method all

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

1;
