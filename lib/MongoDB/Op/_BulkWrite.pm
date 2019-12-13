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
package MongoDB::Op::_BulkWrite;

# Encapsulate a multi-document multi-operation write; returns a
# MongoDB::BulkWriteResult object

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Error;
use MongoDB::BulkWriteResult;
use MongoDB::UnacknowledgedResult;
use MongoDB::Op::_InsertOne;
use MongoDB::Op::_Update;
use MongoDB::Op::_Delete;
use MongoDB::_Protocol;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    Boolish
);
use Types::Standard qw(
    ArrayRef
    InstanceOf
);
use Safe::Isa;
use boolean;

use namespace::clean;

has queue => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef,
);

has ordered => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has client => (
    is => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

has _retryable => (
    is => 'rw',
    isa => Boolish,
    default => 1,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_WriteOp
  MongoDB::Role::_UpdatePreEncoder
  MongoDB::Role::_InsertPreEncoder
  MongoDB::Role::_BypassValidation
);

sub _is_retryable {
    my $self = shift;
    return $self->_should_use_acknowledged_write && $self->_retryable;
}

sub has_collation {
    my $self = shift;
    return !!grep {
        my ( $type, $doc ) = @$_;
        ( $type eq "update" || $type eq "delete" ) && defined $doc->{collation};
    } @{ $self->queue };
}

sub execute {
    my ( $self, $link ) = @_;

    Carp::confess("NO LINK") unless $link;

    if ( $self->has_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" )
          if !$link->supports_collation;

        MongoDB::UsageError->throw(
            "Unacknowledged bulk writes that specify a collation are not allowed")
          if !$self->_should_use_acknowledged_write;
    }

    my $use_write_cmd = $link->supports_write_commands;

    # If using legacy write ops, then there will never be a valid modified_count
    # result so we set that to undef in the constructor; otherwise, we set it
    # to 0 so that results accumulate normally. If a mongos on a mixed topology
    # later fails to set it, results merging will handle it in that case.
    # If unacknowledged, we have to accumulate a result to get bulk semantics
    # right and just throw it away later.
    my $result = MongoDB::BulkWriteResult->_new(
        modified_count       => ( $use_write_cmd ? 0 : undef ),
        write_errors         => [],
        write_concern_errors => [],
        op_count             => 0,
        batch_count          => 0,
        inserted_count       => 0,
        upserted_count       => 0,
        matched_count        => 0,
        deleted_count        => 0,
        upserted             => [],
        inserted             => [],
    );

    my @batches =
        $self->ordered
      ? $self->_batch_ordered( $link, $self->queue )
      : $self->_batch_unordered( $link, $self->queue );

    for my $batch (@batches) {
        if ($use_write_cmd) {
            $self->_execute_write_command_batch( $link, $batch, $result );
        }
        else {
            $self->_execute_legacy_batch( $link, $batch, $result );
        }
    }

    return MongoDB::UnacknowledgedResult->_new(
        write_errors         => [],
        write_concern_errors => [],
    ) if ! $self->_should_use_acknowledged_write;

    # only reach here with an error for unordered bulk ops
    $result->assert_no_write_error;

    # write concern errors are thrown only for the entire batch
    $result->assert_no_write_concern_error;

    return $result;
}

my %OP_MAP = (
    insert => [ insert => 'documents' ],
    update => [ update => 'updates' ],
    delete => [ delete => 'deletes' ],
);

# _execute_write_command_batch may split batches if they are too large and
# execute them separately

sub _execute_write_command_batch {
    my ( $self, $link, $batch, $result ) = @_;

    my ( $type, $docs, $idx_map )   = @$batch;
    my ( $cmd,  $op_key ) = @{ $OP_MAP{$type} };

    my $boolean_ordered = boolean( $self->ordered );
    my ( $db_name, $coll_name, $wc ) =
      map { $self->$_ } qw/db_name coll_name write_concern/;

    my @left_to_send = ($docs);
    my @sending_idx_map = ($idx_map);

    my $max_bson_size = $link->max_bson_object_size;
    my $supports_document_validation = $link->supports_document_validation;

    while (@left_to_send) {
        my $chunk = shift @left_to_send;
        my $chunk_idx_map = shift @sending_idx_map;
        # for update/insert, pre-encode docs as they need custom BSON handling
        # that can't be applied to an entire write command at once
        if ( $cmd eq 'update' ) {
            # take array of hash, validate and encode each update doc; since this
            # might be called more than once if chunks are getting split, check if
            # the update doc is already encoded; this also removes the 'is_replace'
            # field that needs to not be in the command sent to the server
            for ( my $i = 0; $i <= $#$chunk; $i++ ) {
                next if ref( $chunk->[$i]{u} ) eq 'BSON::Raw';
                my $is_replace = delete $chunk->[$i]{is_replace};
                $chunk->[$i]{u} = $self->_pre_encode_update( $max_bson_size, $chunk->[$i]{u}, $is_replace );
            }
        }
        elsif ( $cmd eq 'insert' ) {
            # take array of docs, encode each one while saving original or generated _id
            # field; since this might be called more than once if chunks are getting
            # split, check if the doc is already encoded
            for ( my $i = 0; $i <= $#$chunk; $i++ ) {
                unless ( ref( $chunk->[$i] ) eq 'BSON::Raw' ) {
                    $chunk->[$i] = $self->_pre_encode_insert( $max_bson_size, $chunk->[$i], '.' );
                };
            }
        }

        my $cmd_doc = [
            $cmd         => $coll_name,
            $op_key      => $chunk,
            ordered      => $boolean_ordered,
            @{ $wc->as_args },
        ];

        if ( $cmd eq 'insert' || $cmd eq 'update' ) {
            $cmd_doc = $self->_maybe_bypass( $supports_document_validation, $cmd_doc );
        }

        my $op = MongoDB::Op::_Command->_new(
            db_name             => $db_name,
            query               => $cmd_doc,
            query_flags         => {},
            bson_codec          => $self->bson_codec,
            session             => $self->session,
            retryable_write     => $self->retryable_write,
            monitoring_callback => $self->monitoring_callback,
        );

        my $cmd_result = eval {
            $self->_is_retryable
              ? $self->client->send_retryable_write_op( $op )
              : $self->client->send_write_op( $op );
        } or do {
            my $error = $@ || "Unknown error";
            # This error never touches the database!.... so is before any retryable writes errors etc.
            if ( $error->$_isa("MongoDB::_CommandSizeError") ) {
                if ( @$chunk == 1 ) {
                    MongoDB::DocumentError->throw(
                        message  => "document too large",
                        document => $chunk->[0],
                    );
                }
                else {
                    unshift @left_to_send, $self->_split_chunk( $chunk, $error->size );
                    unshift @sending_idx_map, $self->_split_chunk( $chunk_idx_map, $error->size );
                }
            }
            elsif ( $error->$_can( 'result' ) ) {
                # We are already going to explode from something here, but
                # BulkWriteResult has the correct parsing method to allow us to
                # check for write errors, as they have a higher priority than
                # write concern errors.
                MongoDB::BulkWriteResult->_parse_cmd_result(
                    op       => $type,
                    op_count => scalar @$chunk,
                    result   => $error->result,
                    cmd_doc  => $cmd_doc,
                    idx_map  => $chunk_idx_map,
                )->assert_no_write_error;
                # Explode with original error
                die $error;
            }
            else {
                die $error;
            }
        };

        redo unless $cmd_result; # restart after a chunk split

        my $r = MongoDB::BulkWriteResult->_parse_cmd_result(
            op       => $type,
            op_count => scalar @$chunk,
            result   => $cmd_result,
            cmd_doc  => $cmd_doc,
            idx_map  => $chunk_idx_map,
        );

        # append corresponding ops to errors
        if ( $r->count_write_errors ) {
            for my $error ( @{ $r->write_errors } ) {
                $error->{op} = $chunk->[ $error->{index} ];
            }
        }

        $result->_merge_result($r);
        $result->assert_no_write_error if $boolean_ordered;
    }

    return;
}

sub _split_chunk {
    my ( $self, $chunk, $size ) = @_;

    my $avg_cmd_size       = $size / @$chunk;
    my $new_cmds_per_chunk = int( MAX_BSON_WIRE_SIZE / $avg_cmd_size );

    my @split_chunks;
    while (@$chunk) {
        push @split_chunks, [ splice( @$chunk, 0, $new_cmds_per_chunk ) ];
    }

    return @split_chunks;
}

sub _batch_ordered {
    my ( $self, $link, $queue ) = @_;
    my @batches;
    my $last_type = '';
    my $count     = 0;

    my $max_batch_count = $link->max_write_batch_size;

    my $queue_idx = 0;
    for my $op (@$queue) {
        my ( $type, $doc ) = @$op;
        if ( $type ne $last_type || $count == $max_batch_count ) {
            push @batches, [ $type => [$doc], [$queue_idx] ];
            $last_type = $type;
            $count     = 1;
        }
        else {
            push @{ $batches[-1][1] }, $doc;
            push @{ $batches[-1][2] }, $queue_idx;
            $count++;
        }
        $queue_idx++;
    }

    return @batches;
}

sub _batch_unordered {
    my ( $self, $link, $queue ) = @_;
    my %batches = map { $_ => [ [] ] } keys %OP_MAP;
    my %queue_map = map { $_ => [ [] ] } keys %OP_MAP;

    my $max_batch_count = $link->max_write_batch_size;

    my $queue_idx = 0;
    for my $op (@$queue) {
        my ( $type, $doc ) = @$op;
        if ( @{ $batches{$type}[-1] } == $max_batch_count ) {
            push @{ $batches{$type} }, [$doc];
            push @{ $queue_map{$type} }, [ $queue_idx ];
        }
        else {
            push @{ $batches{$type}[-1] }, $doc;
            push @{ $queue_map{$type}[-1] }, $queue_idx;
        }
        $queue_idx++;
    }

    # insert/update/delete are guaranteed to be in random order on Perl 5.18+
    my @batches;
    for my $type ( grep { scalar @{ $batches{$_}[-1] } } keys %batches ) {
        push @batches, map { [
            $type,
            $batches{$type}[$_],
            $queue_map{$type}[$_], # array of indices from the original queue
        ] } 0 .. $#{ $batches{$type} };
    }
    return @batches;
}

sub _execute_legacy_batch {
    my ( $self, $link, $batch, $result ) = @_;
    my ( $type, $docs ) = @$batch;
    my $ordered = $self->ordered;

    # if write concern is not safe, we have to proxy with a safe one so that
    # we can interrupt ordered bulks, even while ignoring the actual error
    my $wc  = $self->write_concern;
    my $w_0 = !$wc->is_acknowledged;
    if ($w_0) {
        my $wc_args = $wc->as_args();
        my $wcs = scalar @$wc_args ? $wc->as_args()->[1] : {};
        $wcs->{w} = 1;
        $wc = MongoDB::WriteConcern->new($wcs);
    }

    # XXX successive inserts ought to get batched up, up to the max size for
    # batch, but we have no feedback on max size to know how many to put
    # together. I wonder if send_insert should return a list of write results,
    # or if it should just strip out however many docs it can from an arrayref
    # and leave the rest, and then this code can iterate.

    for my $doc (@$docs) {

        my $op;
        if ( $type eq 'insert' ) {
            $op = MongoDB::Op::_InsertOne->_new(
                db_name             => $self->db_name,
                coll_name           => $self->coll_name,
                full_name           => $self->db_name . "." . $self->coll_name,
                document            => $doc,
                write_concern       => $wc,
                bson_codec          => $self->bson_codec,
                monitoring_callback => $self->monitoring_callback,
            );
        }
        elsif ( $type eq 'update' ) {
            $op = MongoDB::Op::_Update->_new(
                db_name             => $self->db_name,
                coll_name           => $self->coll_name,
                full_name           => $self->db_name . "." . $self->coll_name,
                filter              => $doc->{q},
                update              => $doc->{u},
                multi               => $doc->{multi},
                upsert              => $doc->{upsert},
                write_concern       => $wc,
                is_replace          => $doc->{is_replace},
                bson_codec          => $self->bson_codec,
                monitoring_callback => $self->monitoring_callback,
            );
        }
        elsif ( $type eq 'delete' ) {
            $op = MongoDB::Op::_Delete->_new(
                db_name             => $self->db_name,
                coll_name           => $self->coll_name,
                full_name           => $self->db_name . "." . $self->coll_name,
                filter              => $doc->{q},
                just_one            => !!$doc->{limit},
                write_concern       => $wc,
                bson_codec          => $self->bson_codec,
                monitoring_callback => $self->monitoring_callback,
            );
        }

        my $op_result = eval {
            $op->execute($link);
        } or do {
            my $error = $@ || "Unknown error";
            if (   $error->$_isa("MongoDB::DatabaseError")
                && $error->result->does("MongoDB::Role::_WriteResult") )
            {
                return $error->result;
            }
            die $error unless $w_0 && /exceeds maximum size/;
            return undef; ## no critic: this makes op_result undef
        };

        my $gle_result =
          $op_result ? MongoDB::BulkWriteResult->_parse_write_op($op_result) : undef;

        # Even for {w:0}, if the batch is ordered we have to break on the first
        # error, but we don't throw the error to the user.
        if ($w_0) {
            last if $ordered && ( !$gle_result || $gle_result->count_write_errors );
        }
        else {
            $result->_merge_result($gle_result);
            $result->assert_no_write_error if $ordered;
        }
    }

    return;
}

1;
