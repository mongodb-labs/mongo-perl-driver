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
use MongoDB::GridFS;

has _grid => (
    is       => 'ro',
    isa      => 'MongoDB::GridFS',
    required => 1,
);

=attr info

A hash of info information saved with this file.

=cut

has info => (
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
    my ($self, $fh) = @_;

    $self->_grid->chunks->ensure_index(["n"]);

    my $written = 0;
    my $pos = $fh->getpos();

    my $cursor = $self->_grid->chunks->query({"files_id" => $self->info->{"_id"}})->sort({"n" => 1});
    while (my $chunk = $cursor->next) { # && $written < $bytes) {
        $fh->print($chunk->{"data"});
        $written += length $chunk->{'data'};
        #$pos += length $chunk->{'data'};
    }
    $fh->setpos($pos);
    return $written;
}

1;

