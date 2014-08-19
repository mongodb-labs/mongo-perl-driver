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

package MongoDB::GridFS::File;


# ABSTRACT: A Mongo GridFS file

use version;
our $VERSION = 'v0.704.5.1';

use MongoDB::GridFS;
use IO::File;
use Moose;
use namespace::clean -except => 'meta';

=head1 NAME

MongoDB::GridFS::File - A Mongo GridFS file

=head1 SYNOPSIS

    use MongoDB::GridFS::File;

    my $outfile = IO::File->new("outfile", "w");
    my $file = $grid->find_one;
    $file->print($outfile);

=cut

has _grid => (
    is       => 'ro',
    isa      => 'MongoDB::GridFS',
    required => 1,
);

=head1 ATTRIBUTES

=head2 info

A hash of info information saved with this file.

=cut

has info => (
    is => 'ro',
    isa => 'HashRef',
    required => 1,
);

=head1 METHODS

=head2 print ($fh, $length?, $offset?)

    $written = $file->print($fh, 50, 200);

Writes the number of bytes specified from the offset specified 
to the given file handle.  If no C<$length> or C<$offset> are
given, the entire file is written to C<$fh>.  Returns the number
of bytes written.

=cut

sub print {
    my ($self, $fh, $length, $offset) = @_;
    $offset ||= 0;
    $length ||= 0;
    my ($written, $pos) = (0, 0);
    my $start_pos = $fh->getpos();

    $self->_grid->chunks->ensure_index(Tie::IxHash->new(files_id => 1, n => 1), { safe => 1, unique => 1 });

    my $cursor = $self->_grid->chunks->query({"files_id" => $self->info->{"_id"}})->sort({"n" => 1});

    while ((my $chunk = $cursor->next) && (!$length || $written < $length)) {
        my $len = length $chunk->{'data'};

        # if we are cleanly beyond the offset
        if (!$offset || $pos >= $offset) {
            if (!$length || $written + $len < $length) {
                $fh->print($chunk->{"data"});
                $written += $len;
                $pos += $len;
            }
            else {
                $fh->print(substr($chunk->{'data'}, 0, $length-$written));
                $written += $length-$written;
                $pos += $length-$written;
            }
            next;
        }
        # if the offset goes to the middle of this chunk
        elsif ($pos + $len > $offset) {
            # if the length of this chunk is smaller than the desired length
            if (!$length || $len <= $length-$written) {
                $fh->print(substr($chunk->{'data'}, $offset-$pos, $len-($offset-$pos)));
                $written += $len-($offset-$pos);
                $pos += $len-($offset-$pos);
            }
            else {
                $fh->print(substr($chunk->{'data'}, $offset-$pos, $length));
                $written += $length;
                $pos += $length;
            }
            next;
        }
        # if the offset is larger than this chunk
        $pos += $len;
    }
    $fh->setpos($start_pos);
    return $written;
}

=head2 slurp ($length?, $offset?)

    $bytes = $file->slurp(50, 200);
    $all   = $file->slurp

Return the number of bytes specified from the offset specified
to the given file handle.  If no C<$length> or C<$offset> are
given, the entire file is return.

=cut


sub slurp {
    my ($self,$length,$offset) = @_;
    my $bytes = '';
    my $fh = new IO::File \$bytes,'+>';
    my $written = $self->print($fh,$length,$offset);

    # some machines don't set $bytes
    if ($written and !length($bytes)) {
       my $retval;
       read $fh, $retval, $written;
       return $retval;
    }

    return $bytes;
}

__PACKAGE__->meta->make_immutable;

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
