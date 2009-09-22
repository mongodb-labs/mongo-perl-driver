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

package MongoDB::GridFS::File;
# ABSTRACT: A Mongo GridFS file

use Any::Moose;

has _grid => (
    is       => 'ro',
    isa      => 'MongoDB::GridFS',
    required => 1,
);

has meta => (
    is => 'ro',
    isa => 'HashRef',
    required => 1,
);


=method print ($fh)

    $written = $file->print($fh);

Writes the number of bytes specified from the offset specified 
to the given file handle.  If no C<$bytes> or C<$offset> are
given, the entire file is written to C<$fh>.  Returns the number
of bytes written.

=cut

sub print {
    #TODO: bytes, offset
    my ($self, $fh) = $_;

    $self->_grid->chunks->ensure_index("n");

    my $written = 0;
    my $pos = $fh->getpos();
    my $chunk_size = $self->meta{"chunkSize"};

    my $cursor = $self->_grid->chunks->find({"_id" => $self->meta{"_id"}})->sort({"n" => 1});
    while (my $chunk = $cursor->next && $written < $bytes) {
        print $fh, $chunk{"data"};
        $written += $chunk_size;
        $pos += $chunk_size;
    }
    return $written;
}

1;

