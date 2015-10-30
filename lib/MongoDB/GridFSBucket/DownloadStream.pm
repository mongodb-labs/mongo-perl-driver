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
    Maybe
    HashRef
    InstanceOf
    FileHandle
);
use MongoDB::_Types qw(
    NonNegNum
);
use Test::More;
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

has _current_chunk => (
    is => 'rw',
    isa => Maybe[HashRef],
);

has _chunk_location => (
    is => 'rw',
    isa => NonNegNum,
);

has _chunk_length => (
    is => 'rw',
    isa => NonNegNum,
);

has _result => (
    is  => 'lazy',
    isa => Maybe[InstanceOf['MongoDB::QueryResult']],
);

sub _build__result {
    my ($self) = @_;
    return $self->bucket->chunks->find({ files_id => $self->id }, { sort => { n => 1 } })->result;
}

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

sub _ensure_chunk {
    my ($self) = @_;
    if ($self->_current_chunk && $self->_chunk_location < $self->_chunk_length) {
        return $self->_current_chunk;
    }
    return unless $self->_result->has_next;
    $self->_current_chunk($self->_result->next);
    $self->_chunk_location(0);
    $self->_chunk_length(length $self->_current_chunk->{data}->{data});

    return $self->_current_chunk;
}

sub _read_bytes_from_chunk {
    my ($self, $nbytes) = @_;
    return unless $self->_ensure_chunk;

    my $bytes_available = $self->_chunk_length - $self->_chunk_location;
    my $bytes_read = $bytes_available < $nbytes ? $bytes_available : $nbytes;
    my $read = substr $self->_current_chunk->{data}->{data}, $self->_chunk_location, $bytes_read;
    $self->_chunk_location($self->_chunk_location + $bytes_read);

    return ($read, $bytes_read);
}

sub _readline {
    my ($self) = @_;
    return unless $self->_ensure_chunk;

    my $newline_position = index $self->_current_chunk->{data}->{data}, $/, $self->_chunk_location;

    my $bytes_read = $newline_position < 0 ? $self->_chunk_length - $self->_chunk_location : ($newline_position - $self->_chunk_location) + 1;
    my $result = substr $self->_current_chunk->{data}->{data}, $self->_chunk_location, $bytes_read;
    $self->_chunk_location($self->_chunk_location + $bytes_read);

    return ($result, $newline_position < 0 ? 0 : 1);
}

sub readline {
    my ($self) = @_;
    my $result = '';
    my @result_arr = ();
    my ($line, $found_newline) = $self->_readline;
    return unless $line;
    while ($line) {
        while ($line) {
            $result .= $line;
            last if $found_newline;
            ($line, $found_newline) = $self->_readline;
        }
        return $result unless wantarray();
        push @result_arr, $result;
    }
    return @result_arr;
}

sub readbytes {
    my ($self, $nbytes) = @_;
    return unless $self->_ensure_chunk;

    my $result = '';
    my $remaining = $nbytes;
    while ($remaining > 0) {
        my ($tmp, $read) = $self->_read_bytes_from_chunk($remaining);
        last unless $tmp;
        $result .= $tmp;
        $remaining -= $read;
    }

    return $result;
};

sub read {
    my $self = shift;
    return unless $self->_ensure_chunk;
	my $buffref = \$_[0];
	my(undef,$len,$offset) = @_;
    $offset ||= 0;
    my $bufflen = length $$buffref;
    my $pre_str = '';
    if ($offset > 0) {
        if ($offset > $bufflen) {
            $pre_str = $buffref . ("\0" x ($offset - $bufflen));
        } else {
            $pre_str = substr $$buffref, 0, $offset + 1;
        }
    } elsif ($offset < 0) {
        $pre_str = substr $$buffref, 0, $bufflen + $offset;
    }

    my $read = $self->readbytes($len);
    my $read_len = length $read;
    # FIXME: should return undef when empty
    $$buffref = $pre_str . $read;
	return $read_len;
}

# Magic tie methods

sub TIEHANDLE {
    my ($class, $self) = @_;
    return $self;
}

sub READ {
	my $self = shift;
    my $buffref = \$_[0];
	my(undef,$len,$offset) = @_;
    return $self->read($$buffref, $len, $offset);
}

sub GETC {
	my ($self) = @_;
    return $self->readbytes(1);
}

sub READLINE {
	my ($self) = @_;
    return $self->readline;
}

1;
