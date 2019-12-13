#  Copyright 2014 - present MongoDB, Inc.
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

use strict;
use warnings;
package MongoDB::Role::_CommandCursorOp;

# MongoDB interface for database commands with cursors

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::Error;
use MongoDB::QueryResult;

use namespace::clean;

requires qw/session client bson_codec/;

sub _build_result_from_cursor {
    my ( $self, $res ) = @_;

    my $c = $res->output->{cursor}
      or MongoDB::DatabaseError->throw(
        message => "no cursor found in command response",
        result  => $res,
      );

    my $max_time_ms = 0;
    if ($self->isa('MongoDB::Op::_Query') &&
        $self->options->{cursorType} eq 'tailable_await') {
        $max_time_ms = $self->options->{maxAwaitTimeMS} if $self->options->{maxAwaitTimeMS};
    }
    elsif (
        $self->isa('MongoDB::Op::_Aggregate') ||
        $self->isa('MongoDB::Op::_ChangeStream')
    ) {
        $max_time_ms = $self->maxAwaitTimeMS if $self->maxAwaitTimeMS;
    }

    my $limit = 0;
    if ($self->isa('MongoDB::Op::_Query')) {
        $limit = $self->options->{limit} if $self->options->{limit};
    }

    my $batch = $c->{firstBatch};
    my $qr = MongoDB::QueryResult->_new(
        _client       => $self->client,
        _address      => $res->address,
        _full_name    => $c->{ns},
        _bson_codec   => $self->bson_codec,
        _batch_size   => scalar @$batch,
        _cursor_at    => 0,
        _limit        => $limit,
        _cursor_id    => $c->{id},
        _cursor_start => 0,
        _cursor_flags => {},
        _cursor_num   => scalar @$batch,
        _docs         => $batch,
        _max_time_ms  => $max_time_ms,
        _session       => $self->session,
	_post_batch_resume_token => $c->{postBatchResumeToken},
    );
}

sub _empty_query_result {
    my ( $self, $link ) = @_;

    my $qr = MongoDB::QueryResult->_new(
        _client       => $self->client,
        _address      => $link->address,
        _full_name    => '',
        _bson_codec   => $self->bson_codec,
        _batch_size   => 1,
        _cursor_at    => 0,
        _limit        => 0,
        _cursor_id    => 0,
        _cursor_start => 0,
        _cursor_flags => {},
        _cursor_num   => 0,
        _docs         => [],
    );
}

1;
