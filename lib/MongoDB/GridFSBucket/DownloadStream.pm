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

package MongoDB::GridFSBucket::DownloadStream;

use Moo;
use Types::Standard qw(
    Str
    Maybe
    HashRef
    InstanceOf
    FileHandle
);
use MongoDB::_Types qw(
    NonNegNum
);
use List::Util qw(max min);
use namespace::clean -except => 'meta';

has bucket => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::GridFSBucket'],
    required => 1,
);

has id => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::OID'],
    required => 1,
);

has file_doc => (
    is       => 'ro',
    isa      => HashRef,
    required => 1,
);

has _buffer => (
    is => 'rw',
    isa => Str,
);

has _chunk_n => (
    is      => 'rw',
    isa     => NonNegNum,
    default => sub { 0 },
);

has _result => (
    is       => 'ro',
    isa      => Maybe[InstanceOf['MongoDB::QueryResult']],
    required => 1,
);

has _offset => (
    is      => 'rw',
    isa     => NonNegNum,
    default => sub { 0 },
);

has fh => (
    is => 'lazy',
    isa => FileHandle,
);

sub _build_fh {
    my ($self) = @_;
    my $fh = IO::Handle->new();
    tie *$fh, 'MongoDB::GridFSBucket::DownloadStream', $self;
    return $fh;
}

sub _get_next_chunk {
    my ($self) = @_;

    return unless $self->_result && $self->_result->has_next;
    my $chunk = $self->_result->next;

    if ( $chunk->{'n'} != $self->_chunk_n ) {
        MongoDB::GridFSError->throw(sprintf(
                'Expected chunk %d but got chunk %d',
                $self->_chunk_n, $chunk->{'n'},
        ));
    }

    my $last_chunk_n = int($self->file_doc->{'length'} / $self->file_doc->{'chunkSize'});
    my $expected_size = $chunk->{'n'} == $last_chunk_n ?
            $self->file_doc->{'length'} % $self->file_doc->{'chunkSize'} :
            $self->file_doc->{'chunkSize'};
    if (length $chunk->{'data'} != $expected_size ) {
        MongoDB::GridFSError->throw(sprintf(
            "Chunk %d from file with id %s has incorrect size %d, expected %d",
            $self->_chunk_n, $self->id, length $chunk->{'data'}, $expected_size,
        ));
    }

    $self->{_chunk_n} += 1;
    $self->{_buffer} .= $chunk->{data}->{data};
}

sub _ensure_buffer {
    my ($self) = @_;
    if ($self->_buffer) { return $self->_buffer };

    $self->_get_next_chunk;

    return $self->_buffer;
}

sub _readline_scalar {
    my ($self) = @_;

    # Special case for "slurp" mode
    if ( !$/ ) {
        my $result;
        $self->read($result, $self->file_doc->{'length'});
        return $result;
    }

    return unless $self->_ensure_buffer;
    my $newline_index;
    while ( ($newline_index = index $self->_buffer, $/) < 0) { last unless $self->_get_next_chunk };
    my $substr_len = $newline_index < 0 ? length $self->_buffer : $newline_index + 1;
    return substr $self->{_buffer}, $self->_offset, $substr_len, '';
}

sub readline {
    my ($self) = @_;
    return $self->_readline_scalar unless wantarray;

    my @result = ();
    while ( my $line = $self->_readline_scalar ) {
        push @result, $line;
    }
    return @result;
}

sub read {
    my $self = shift;
    return unless $self->_ensure_buffer;
	my $buffref = \$_[0];
	my(undef,$len,$offset) = @_;
    my $bufflen = length $$buffref;

    $offset ||= 0;
    $bufflen ||= 0;
    $$buffref ||= '';

    $offset = max(0, $bufflen + $offset) if $offset < 0;
    if ($offset > 0 && $offset > $bufflen) {
        $$buffref .= ("\0" x ($offset - $bufflen));
    } else {
        substr $$buffref, $offset, $bufflen, '';
    }

    while ( length $self->_buffer < $len ) { last unless $self->_get_next_chunk };
    my $read_len = min(length $self->_buffer, $len);
    $$buffref .= substr $self->{_buffer}, $self->_offset, $read_len, '';
	return $read_len;
}

sub close {
    my ($self) = @_;
    $self->{_result} = undef;
    $self->_buffer('');
    $self->_chunk_n(0);
    $self->{fh} = undef;
}

# Magic tie methods

sub TIEHANDLE {
    my ($class, $self) = @_;
    return $self;
}

*READ = \&read;
*READLINE = \&readline;
*CLOSE = \&close;

sub GETC {
	my ($self) = @_;
    my $char;
    $self->read($char, 1);
    return $char;
}

1;
