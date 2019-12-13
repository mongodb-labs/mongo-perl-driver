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
package MongoDB::Op::_KillCursors;

# Encapsulate a cursor kill operation; returns true

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::_Protocol;
use Types::Standard qw(
    ArrayRef
);

use namespace::clean;

has cursor_ids => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef,
);

with $_ for qw(
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_DatabaseOp
  MongoDB::Role::_PrivateConstructor
);

sub execute {
    my ( $self, $link ) = @_;

    if ( $link->supports_query_commands ) {
        # Spec says that failures should be ignored: cursor kills often happen
        # via destructors and users can't do anything about failure anyway.
        eval {
            MongoDB::Op::_Command->_new(
                db_name => $self->db_name,
                query   => [
                    killCursors => $self->coll_name,
                    cursors     => $self->cursor_ids,
                ],
                query_flags         => {},
                bson_codec          => $self->bson_codec,
                session             => $self->session,
                monitoring_callback => $self->monitoring_callback,
            )->execute($link);
        };
    }
    # Server never sends a reply, so ignoring failure here is automatic.
    else {
        my ($msg, $request_id) = MongoDB::_Protocol::write_kill_cursors(
            @{ $self->cursor_ids },
        );

        my $start_event;
        $start_event = $self->_legacy_publish_command_started(
            $link,
            $request_id,
        ) if $self->monitoring_callback;
        my $start = time;

        eval {
            $link->write($msg);
        };

        my $duration = time - $start;
        if (my $err = $@) {
            $self->_legacy_publish_command_exception(
                $start_event,
                $duration,
                $err,
            ) if $self->monitoring_callback;
            die $err;
        }

        $self->_legacy_publish_command_reply($start_event, $duration)
            if $self->monitoring_callback;
    }

    return 1;
}

sub _legacy_publish_command_started {
    my ($self, $link, $request_id) = @_;

    my %cmd;
    tie %cmd, "Tie::IxHash", (
        killCursors => $self->coll_name,
        cursors     => $self->cursor_ids,
    );

    my $event = {
        type         => 'command_started',
        databaseName => $self->db_name,
        commandName  => 'killCursors',
        command      => \%cmd,
        requestId    => $request_id,
        connectionId => $link->address,
    };

    eval { $self->monitoring_callback->($event) };

    return $event;
}

sub _legacy_publish_command_exception {
    my ($self, $start_event, $duration, $err) = @_;

    my $event = {
        type         => 'command_failed',
        databaseName => $start_event->{databaseName},
        commandName  => $start_event->{commandName},
        requestId    => $start_event->{requestId},
        connectionId => $start_event->{connectionId},
        durationSecs => $duration,
        reply        => {},
        failure      => "$err",
        eval_error   => $err,
    };

    eval { $self->monitoring_callback->($event) };

    return;
}

sub _legacy_publish_command_reply {
    my ($self, $start_event, $duration) = @_;

    my $event = {
        type         => 'command_succeeded',
        databaseName => $start_event->{databaseName},
        commandName  => $start_event->{commandName},
        requestId    => $start_event->{requestId},
        connectionId => $start_event->{connectionId},
        durationSecs => $duration,
        reply        => {
            ok => 1,
            cursorsUnknown => $self->cursor_ids,
        },
    };

    eval { $self->monitoring_callback->($event) };

    return;
}

1;
