#  Copyright 2018 - present MongoDB, Inc.
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

package MongoDB::Role::_CommandMonitoring;

# MongoDB role to add command monitoring support to Ops

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;
use BSON;
use BSON::Raw;
use MongoDB::_Types -types, 'to_IxHash';
use Tie::IxHash;
use Safe::Isa;
use Time::HiRes qw/time/;
use namespace::clean;

requires qw/monitoring_callback db_name/;
has command_start_time  => ( is => 'rw', );
has command_start_event => ( is => 'rw', );

sub publish_command_started {
    my ( $self, $link, $command, $request_id ) = @_;
    return unless $self->monitoring_callback;

    if ( $command->$_can('_as_tied_hash') ) {
        $command = $command->_as_tied_hash;
    } else {
        $command = _to_tied_ixhash($command);
    }
    my $command_name = tied(%$command)->Keys(0);

    my $event = {
        type         => 'command_started',
        databaseName => $self->db_name,
        commandName  => $command_name,
        command      => (
            _needs_redaction($command_name)
                ? _to_tied_ixhash([])
                : $command,
        ),
        requestId    => $request_id,
        connectionId => $link->address,
    };

    # Cache for constructing matching succeeded/failed event later
    $self->command_start_event($event);

    # Guard against exceptions in the callback
    eval { $self->monitoring_callback->($event) };

    # Set the time last so it doesn't include all the work above
    $self->command_start_time(time);
    return;
}

sub publish_command_reply {
    my ( $self, $bson ) = @_;
    return unless $self->monitoring_callback;

    # Record duration early before doing work to prepare success/fail
    # events
    my $duration = time - $self->command_start_time();

    my $start_event = $self->command_start_event();

    my $reply =
      ref($bson) eq 'HASH'
      ? $bson
      : BSON->new()->decode_one($bson);

    my $event = {
        databaseName => $start_event->{databaseName},
        commandName  => $start_event->{commandName},
        requestId    => $start_event->{requestId},
        connectionId => $start_event->{connectionId},
        durationSecs => $duration,
        reply        => (
            _needs_redaction($start_event->{commandName})
                ? {}
                : $reply,
        ),
    };

    if ( $reply->{ok} ) {
        $event->{type} = 'command_succeeded';
    }
    else {
        $event->{type}   = 'command_failed';
        $event->{failure} = _extract_errmsg($reply);
    }

    # Guard against exceptions in the callback
    eval { $self->monitoring_callback->($event) };

    return;
}

sub publish_command_exception {
    my ($self, $err) = @_;
    return unless $self->monitoring_callback;

    # Record duration early before doing work to prepare success/fail
    # events
    my $duration = time - $self->command_start_time();

    my $start_event = $self->command_start_event();

    my $event = {
        type         => "command_failed",
        databaseName => $start_event->{databaseName},
        commandName  => $start_event->{commandName},
        requestId    => $start_event->{requestId},
        connectionId => $start_event->{connectionId},
        durationSecs => $duration,
        reply        => {},
        failure      => "$err",
        eval_error   => $err,
    };

    # Guard against exceptions in the callback
    eval { $self->monitoring_callback->($event) };

    return;
}

sub publish_legacy_write_started {
    my ( $self, $link, $cmd_name, $op_doc, $request_id ) = @_;
    my $method = "_convert_legacy_$cmd_name";
    return $self->publish_command_started( $link, $self->$method($op_doc), $request_id );
}

sub publish_legacy_reply_succeeded {
    my ($self, $result) = @_;
    my $batchfield = ref($self) eq "MongoDB::Op::_Query" ? "firstBatch" : "nextBatch";

    my $reply = {
        ok => 1,
        cursor => {
            id => $result->{cursor_id},
            ns => $self->full_name,
            $batchfield => [ @{$result->{docs}} ],
        },
    };

    return $self->publish_command_reply($reply);
}

sub publish_legacy_query_error {
    my ($self, $result) = @_;

    my $reply = {
        %$result,
        ok => 0,
    };

    return $self->publish_command_reply($reply);
}

sub _needs_redaction {
    my ($name) = @_;
    return 1 if grep { $name eq $_ } qw(
        authenticate
        saslStart
        saslContinue
        getnonce
        createUser
        updateUser
        copydbgetnonce
        copydbsaslstart
        copydb
    );
    return 0;
}

sub _convert_legacy_insert {
    my ( $self, $op_doc ) = @_;
    $op_doc = [$op_doc] unless ref $op_doc eq 'ARRAY';
    return [
        insert    => $self->coll_name,
        documents => $op_doc,
        @{ $self->write_concern->as_args },
    ];
}

# Duplicated from MongoDB::CommandResult
sub _extract_errmsg {
    my ($output) = @_;
    for my $err_key (qw/$err err errmsg/) {
        return $output->{$err_key} if exists $output->{$err_key};
    }
    if ( exists $output->{writeConcernError} ) {
        return $output->{writeConcernError}{errmsg};
    }
    return "";
}

sub _convert_legacy_update {
    my ( $self, $op_doc ) = @_;

    return [
        update  => $self->coll_name,
        updates => [
            update  => $self->coll_name,
            updates => [$op_doc],
        ],
        @{ $self->write_concern->as_args },
    ];
}

sub _convert_legacy_delete {
    my ( $self, $op_doc ) = @_;

    return [
        delete  => $self->coll_name,
        deletes => [$op_doc],
        @{ $self->write_concern->as_args },
    ];
}

sub _decode_preencoded {
    my ($obj) = @_;
    my $codec = BSON->new;
    my $type  = ref($obj);
    if ( $type eq 'BSON::Raw' ) {
        return $codec->decode_one( $obj->{bson} );
    }
    elsif ( $type eq 'Tie::IxHash' ) {
        tie my %out, "Tie::IxHash";
        $out{$_} = _decode_preencoded( $obj->FETCH($_) ) for $obj->Keys;
        return \%out;
    }
    elsif ( $type eq 'ARRAY' ) {
        return [ map { _decode_preencoded($_) } @$obj ];
    }
    elsif ( $type eq 'HASH' ) {
        return { map { ; $_ => _decode_preencoded( $obj->{$_} ) } keys %$obj };
    }
    return $obj;
}

sub _to_tied_ixhash {
    my ($in) = @_;
    my $type = ref($in);
    my %out;
    if ( $type eq 'ARRAY' ) {
        # earlier type checks should ensure even elements
        tie %out, "Tie::IxHash", map { _decode_preencoded($_) } @$in;
    }
    elsif ( $type eq "Tie::IxHash" ) {
        tie %out, "Tie::IxHash";
        $out{$_} = _decode_preencoded( $in->FETCH($_) ) for $in->Keys;
    }
    elsif ( $in->$_can('_as_tied_hash') ) {
        %out = %{ $in->_as_tied_hash() };
    } else {
        tie %out, "Tie::IxHash", map { ; $_ => _decode_preencoded( $in->{$_} ) } keys %$in;
    }
    return \%out;
}

1;
