#
#  Copyright 2014 MongoDB, Inc.
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

package MongoDB::Role::_CommandCursorOp;

# MongoDB interface for database commands with cursors

use version;
our $VERSION = 'v0.999.999.7';

use MongoDB::Error;
use MongoDB::QueryResult;
use Moo::Role;

use namespace::clean;

requires qw/client bson_codec/;

sub _build_result_from_cursor {
    my ( $self, $res ) = @_;

    my $c = $res->output->{cursor}
      or MongoDB::DatabaseError->throw(
        message => "no cursor found in command response",
        result  => $res,
      );

    my $max_time_ms = undef;
    if (defined $self->{cursor_type} &&
        $self->{cursor_type} eq 'tailable_await') {
        $max_time_ms = $self->max_time_ms if defined $self->max_time_ms;
    }

    my $batch = $c->{firstBatch};
    my $qr = MongoDB::QueryResult->_new(
        _client      => $self->client,
        _address      => $res->address,
        _ns           => $c->{ns},
        _bson_codec   => $self->bson_codec,
        _batch_size   => scalar @$batch,
        _cursor_at    => 0,
        _limit        => 0,
        _cursor_id    => $c->{id},
        _cursor_start => 0,
        _cursor_flags => {},
        _cursor_num   => scalar @$batch,
        _docs        => $batch,
        defined $max_time_ms ? (_max_time_ms => $max_time_ms) : (),
    );
}

sub _empty_query_result {
    my ( $self, $link ) = @_;

    my $qr = MongoDB::QueryResult->_new(
        _client      => $self->client,
        _address      => $link->address,
        _ns           => '',
        _bson_codec   => $self->bson_codec,
        _batch_size   => 1,
        _cursor_at    => 0,
        _limit        => 0,
        _cursor_id    => 0,
        _cursor_start => 0,
        _cursor_flags => {},
        _cursor_num   => 0,
        _docs        => [],
    );
}

1;
