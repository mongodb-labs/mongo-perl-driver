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
    Int
    Str
    HashRef
    InstanceOf
);
use Test::More;
use namespace::clean -except => 'meta';

has bucket => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::GridFSBucket'],
    required => 1,
);

has _id => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::OID'],
    required => 1,
);

has _result => (
    is  => 'lazy',
    isa => InstanceOf['MongoDB::QueryResult'],
);

has _current_chunk => (
    is => 'rwp',
    isa => HashRef,
);

has _chunk_location => (
    is => 'rwp',
    isa => Int,
);

sub _build__result {
    my ($self) = @_;
    return $self->bucket->chunks->find({ files_id => $self->_id }, { sort => { n => 1 } })->result;
}

sub _ensure_chunk {
    my ($self) = @_;
    return $self->_current_chunk if $self->_current_chunk;
    $self->{_current_chunk} = $self->_result->next;
    $self->{_chunk_location} = 0;

    return $self->_current_chunk;
}

sub _read_bytes_from_chunk {
    my ($self, $nbytes) = @_;
    return unless $self->_ensure_chunk;

    my $bytes_available = (length $self->_current_chunk->{data}->{data}) - $self->_chunk_location;
    my $bytes_read = $bytes_available < $nbytes ? $bytes_available : $nbytes;
    my $read = substr $self->_current_chunk->{data}->{data}, $self->_chunk_location, $bytes_read;
    $self->{_chunk_location} += $bytes_read;
    if ($self->_chunk_location >= length $self->_current_chunk->{data}->{data}) {
        $self->{_current_chunk} = undef;
        $self->{_chunk_location} = -1;
    }

    return ($read, $bytes_read);
}

sub read {
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

1;
