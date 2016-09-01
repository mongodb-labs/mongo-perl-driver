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

package MongoDB::Op::_BulkWrite;

# Encapsulate a multi-document multi-operation write; returns a
# MongoDB::BulkWriteResult object

use version;
our $VERSION = 'v1.5.0';

use Moo;

use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::BulkWriteResult;
use MongoDB::UnacknowledgedResult;
use MongoDB::Op::_InsertOne;
use MongoDB::Op::_Update;
use MongoDB::Op::_Delete;
use MongoDB::_Protocol;
use MongoDB::_Constants;
use Types::Standard qw(
    ArrayRef
    Bool
);
use Safe::Isa;
use Try::Tiny;
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
    isa      => Bool,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_WriteOp
  MongoDB::Role::_UpdatePreEncoder
  MongoDB::Role::_InsertPreEncoder
  MongoDB::Role::_BypassValidation
);

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
          if !$self->write_concern->is_acknowledged;
    }

    my $use_write_cmd = $link->does_write_commands;

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
    ) if !$self->write_concern->is_acknowledged;

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

    my ( $type, $docs )   = @$batch;
    my ( $cmd,  $op_key ) = @{ $OP_MAP{$type} };

    my $boolean_ordered = boolean( $self->ordered );
    my ( $db_name, $coll_name, $wc ) =
      map { $self->$_ } qw/db_name coll_name write_concern/;

    my @left_to_send = ($docs);

    while (@left_to_send) {
        my $chunk = shift @left_to_send;

        # for update/insert, pre-encode docs as they need custom BSON handling
        # that can't be applied to an entire write command at once
        if ( $cmd eq 'update' ) {
            # take array of hash, validate and encode each update doc; since this
            # might be called more than once if chunks are getting split, check if
            # the update doc is already encoded; this also removes the 'is_replace'
            # field that needs to not be in the command sent to the server
            for ( my $i = 0; $i <= $#$chunk; $i++ ) {
                next if ref( $chunk->[$i]{u} ) eq 'MongoDB::BSON::_EncodedDoc';
                my $is_replace = delete $chunk->[$i]{is_replace};
                $chunk->[$i]{u} = $self->_pre_encode_update( $link, $chunk->[$i]{u}, $is_replace );
            }
        }
        elsif ( $cmd eq 'insert' ) {
            # take array of docs, encode each one while saving original or generated _id
            # field; since this might be called more than once if chunks are getting
            # split, check if the doc is already encoded
            for ( my $i = 0; $i <= $#$chunk; $i++ ) {
                unless ( ref( $chunk->[$i] ) eq 'MongoDB::BSON::_EncodedDoc' ) {
                    $chunk->[$i] = $self->_pre_encode_insert( $link, $chunk->[$i], '.' );
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
            (undef, $cmd_doc) = $self->_maybe_bypass($link, $cmd_doc);
        }

        my $op = MongoDB::Op::_Command->_new(
            db_name     => $db_name,
            query       => $cmd_doc,
            query_flags => {},
            bson_codec  => $self->bson_codec,
        );

        my $cmd_result = try {
            $op->execute($link)
        }
        catch {
            if ( $_->$_isa("MongoDB::_CommandSizeError") ) {
                if ( @$chunk == 1 ) {
                    MongoDB::DocumentError->throw(
                        message  => "document too large",
                        document => $chunk->[0],
                    );
                }
                else {
                    unshift @left_to_send, $self->_split_chunk( $link, $chunk, $_->size );
                }
            }
            else {
                die $_;
            }
            return;
        };

        redo unless $cmd_result; # restart after a chunk split

        my $r = MongoDB::BulkWriteResult->_parse_cmd_result(
            op       => $type,
            op_count => scalar @$chunk,
            result   => $cmd_result,
            cmd_doc  => $cmd_doc,
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
    my ( $self, $link, $chunk, $size ) = @_;

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

    for my $op (@$queue) {
        my ( $type, $doc ) = @$op;
        if ( $type ne $last_type || $count == $max_batch_count ) {
            push @batches, [ $type => [$doc] ];
            $last_type = $type;
            $count     = 1;
        }
        else {
            push @{ $batches[-1][-1] }, $doc;
            $count++;
        }
    }

    return @batches;
}

sub _batch_unordered {
    my ( $self, $link, $queue ) = @_;
    my %batches = map { ; $_ => [ [] ] } keys %OP_MAP;

    my $max_batch_count = $link->max_write_batch_size;

    for my $op (@$queue) {
        my ( $type, $doc ) = @$op;
        if ( @{ $batches{$type}[-1] } == $max_batch_count ) {
            push @{ $batches{$type} }, [$doc];
        }
        else {
            push @{ $batches{$type}[-1] }, $doc;
        }
    }

    # insert/update/delete are guaranteed to be in random order on Perl 5.18+
    my @batches;
    for my $type ( grep { scalar @{ $batches{$_}[-1] } } keys %batches ) {
        push @batches, map { [ $type => $_ ] } @{ $batches{$type} };
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
                db_name       => $self->db_name,
                coll_name     => $self->coll_name,
                full_name     => $self->db_name . "." . $self->coll_name,
                document      => $doc,
                write_concern => $wc,
                bson_codec    => $self->bson_codec,
            );
        }
        elsif ( $type eq 'update' ) {
            $op = MongoDB::Op::_Update->_new(
                db_name       => $self->db_name,
                coll_name     => $self->coll_name,
                full_name     => $self->db_name . "." . $self->coll_name,
                filter        => $doc->{q},
                update        => $doc->{u},
                multi         => $doc->{multi},
                upsert        => $doc->{upsert},
                write_concern => $wc,
                is_replace    => $doc->{is_replace},
                bson_codec    => $self->bson_codec,
            );
        }
        elsif ( $type eq 'delete' ) {
            $op = MongoDB::Op::_Delete->_new(
                db_name       => $self->db_name,
                coll_name     => $self->coll_name,
                full_name     => $self->db_name . "." . $self->coll_name,
                filter        => $doc->{q},
                just_one      => !!$doc->{limit},
                write_concern => $wc,
                bson_codec    => $self->bson_codec,
            );
        }

        my $op_result = try {
            $op->execute($link);
        }
        catch {
            if (   $_->$_isa("MongoDB::DatabaseError")
                && $_->result->does("MongoDB::Role::_WriteResult") )
            {
                return $_->result;
            }
            die $_ unless $w_0 && /exceeds maximum size/;
            return undef;
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
