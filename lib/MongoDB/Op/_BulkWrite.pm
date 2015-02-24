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
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;

use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::BulkWriteResult;
use MongoDB::Op::_InsertOne;
use MongoDB::Op::_Update;
use MongoDB::Op::_Delete;
use MongoDB::_Protocol;
use MongoDB::_Types -types;
use Types::Standard -types;
use Safe::Isa;
use Scalar::Util qw/blessed reftype/;
use Tie::IxHash;
use Try::Tiny;
use boolean;
use namespace::clean -except => 'meta';

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has coll_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has queue => (
    is       => 'ro',
    isa      => ArrayRef,
    required => 1,
);

has ordered => (
    is      => 'ro',
    isa     => Bool,
    default => 1,
);

has write_concern => (
    is       => 'ro',
    isa      => WriteConcern,
    coerce   => 1,
    required => 1,
);

# not _WriteOp because we construct our own result objects
with qw/MongoDB::Role::_CommandOp/;

sub execute {
    my ( $self, $link ) = @_;

    my $use_write_cmd = $link->accepts_wire_version(2);

    # If using legacy write ops, then there will never be a valid modified_count
    # result so we set that to undef in the constructor; otherwise, we set it
    # to 0 so that results accumulate normally. If a mongos on a mixed topology
    # later fails to set it, results merging will handle it in that case.
    my $result =
      MongoDB::BulkWriteResult->new( modified_count => $use_write_cmd ? 0 : undef );

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

        my $cmd_doc = [
            $cmd         => $coll_name,
            $op_key      => $chunk,
            ordered      => $boolean_ordered,
            writeConcern => $wc->as_struct,
        ];

        my $op = MongoDB::Op::_Command->new(
            db_name => $db_name,
            query   => $cmd_doc,
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

    my $max_wire_size = $self->MAX_BSON_WIRE_SIZE; # XXX blech

    my $avg_cmd_size       = $size / @$chunk;
    my $new_cmds_per_chunk = int( $max_wire_size / $avg_cmd_size );

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
    my $w_0 = !$wc->is_safe;
    if ($w_0) {
        my $wcs = $wc->as_struct;
        $wcs->{w} = 1;
        $wc = MongoDB::WriteConcern->new($wcs);
    }

    # XXX successive inserts ought to get batched up, up to the max size for
    # batch, but we have no feedback on max size to know how many to put
    # together. I wonder if send_insert should return a list of write results,
    # or if it should just strip out however many docs it can from an arrayref
    # and leave the rest, and then this code can iterate.

    for my $doc (@$docs) {

        # legacy server doesn't check keys on insert; we fake an error if it
        # happens

        if ( $type eq 'insert' && ( my $r = $self->_check_no_dollar_keys($doc) ) ) {
            if ($w_0) {
                last if $ordered;
            }
            else {
                $result->_merge_result($r);
                $result->assert_no_write_error if $ordered;
            }
            next;
        }

        my $op;
        if ( $type eq 'insert' ) {
            $op = MongoDB::Op::_InsertOne->new(
                db_name       => $self->db_name,
                coll_name     => $self->coll_name,
                document      => $doc,
                write_concern => $wc,
            );
        }
        elsif ( $type eq 'update' ) {
            $op = MongoDB::Op::_Update->new(
                db_name       => $self->db_name,
                coll_name     => $self->coll_name,
                filter        => $doc->{q},
                update        => $doc->{u},
                multi         => $doc->{multi},
                upsert        => $doc->{upsert},
                write_concern => $wc,
            );
        }
        elsif ( $type eq 'delete' ) {
            $op = MongoDB::Op::_Delete->new(
                db_name       => $self->db_name,
                coll_name     => $self->coll_name,
                filter        => $doc->{q},
                just_one      => !!$doc->{limit},
                write_concern => $wc,
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

sub _check_no_dollar_keys {
    my ( $self, $doc ) = @_;

    my @keys = ref $doc eq 'Tie::IxHash' ? $doc->Keys : keys %$doc;
    if ( my @bad = grep { substr( $_, 0, 1 ) eq '$' } @keys ) {
        my $errdoc = {
            index  => 0,
            errmsg => "Document can't have '\$' prefixed field names: @bad",
            code   => UNKNOWN_ERROR
        };

        return MongoDB::BulkWriteResult->new(
            op_count       => 1,
            modified_count => undef,
            write_errors   => [$errdoc]
        );
    }

    return;
}

1;
