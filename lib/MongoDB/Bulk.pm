#
#  Copyright 2009-2013 MongoDB, Inc.
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

package MongoDB::Bulk;

# ABSTRACT: MongoDB bulk write interface

use version;
our $VERSION = 'v0.703.5'; # TRIAL

use MongoDB;
use MongoDB::Error;
use MongoDB::OID;
use MongoDB::WriteResult;
use MongoDB::WriteSelector;
use Try::Tiny;
use Safe::Isa;

use Moose;
use namespace::clean -except => 'meta';

=attr collection (required)

The L<MongoDB::Collection> where the operations are to be performed.

=cut

has 'collection' => (
    is       => 'ro',
    isa      => 'MongoDB::Collection',
    required => 1,
    handles  => [qw/name/],
);

=attr ordered (required)

A boolean for whether or not operations should be ordered (true) or
unordered (false).

=cut

has 'ordered' => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
);

has '_executed' => (
    is       => 'rw',
    isa      => 'Bool',
    init_arg => undef,
    default  => 0,
);

has '_ops' => (
    is       => 'rw',
    isa      => 'ArrayRef[ArrayRef]',
    init_arg => undef,
    default  => sub { [] },
    traits   => ['Array'],
    handles  => {
        _enqueue_op => 'push',
        _all_ops    => 'elements',
        _count_ops  => 'count',
        _clear_ops  => 'clear',
    }
);

has '_use_write_cmd' => (
    is         => 'ro',
    isa        => 'Bool',
    lazy_build => 1,
);

sub _build__use_write_cmd {
    my ($self) = @_;
    my $use_it = $self->collection->_database->_client->_use_write_cmd;
    return $use_it;
}

with 'MongoDB::Role::_OpQueue';

sub find {
    my ( $self, $selector ) = @_;

    # XXX replace this with a proper argument check for acceptable selector types
    confess "find requires a criteria document. Use an empty hashref for no criteria."
      unless ref $selector eq 'HASH';

    return MongoDB::WriteSelector->new(
        query    => $selector,
        op_queue => $self,
    );
}

sub insert {
    my ( $self, $doc ) = @_;
    # XXX eventually, need to support array or IxHash
    unless ( @_ == 2 && ref $doc eq 'HASH' ) {
        confess "argument to insert must be a single hash reference";
    }

    $doc->{_id} = MongoDB::OID->new unless exists $doc->{_id};
    $self->_enqueue_op( [ insert => $doc ] );
    return $self;
}

=method execute

    $bulk->execute;

Returns a hash reference with results of the bulk operations.  Keys may
include:

=for :list
* ...
* writeErrors
* writeConcernError

This method will throw an error if there are communication or other serious
errors.  It will not throw error for C<writeErrors> or C<writeConcernError>.
You must check the results document for those.

XXX discuss how order affects errors

=cut

my %OP_MAP = (
    insert => [ insert => 'documents' ],
    update => [ update => 'updates' ],
    delete => [ delete => 'deletes' ],
);

sub execute {
    my ($self) = @_;
    if ( $self->_executed ) {
        MongoDB::Error->throw("bulk op execute called more than once");
    }
    else {
        $self->_executed(1);
    }

    my $ordered = $self->ordered;
    my $result  = MongoDB::WriteResult->new;

    unless ( $self->_count_ops ) {
        MongoDB::Error->throw("no bulk ops to execute");
    }

    for my $batch ( $ordered ? $self->_batch_ordered : $self->_batch_unordered ) {
        if ( $self->_use_write_cmd ) {
            $self->_execute_write_command_batch( $batch, $result, $ordered );
        }
        else {
            $self->_execute_legacy_batch( $batch, $result, $ordered );
        }
    }

    # only reach here with an error for unordered bulk ops
    $self->_assert_no_error($result);

    return $result;
}

# _execute_write_command_batch may split batches if they are too large and
# execute them separately

sub _execute_write_command_batch {
    my ( $self, $batch, $result, $ordered ) = @_;

    my ( $type, $docs )   = @$batch;
    my ( $cmd,  $op_key ) = @{ $OP_MAP{$type} };

    my $boolean_ordered = $ordered ? boolean::true : boolean::false;
    my $coll_name = $self->name;

    my @left_to_send = ($docs);

    while (@left_to_send) {
        my $chunk = shift @left_to_send;

        my $cmd_doc = [
            $cmd    => $coll_name,
            $op_key => $chunk,
            ordered => $boolean_ordered,
        ];

        my $cmd_result = try {
            $self->collection->_database->_try_run_command($cmd_doc);
        }
        catch {
            if ( $_->$_isa("MongoDB::_CommandSizeError") ) {
                if ( @$chunk == 1 ) {
                    # XXX need a proper exception
                    die "document too large";
                }
                else {
                    unshift @left_to_send, $self->_split_chunk( $chunk, $_->size );
                }
            }
            else {
                die $_;
            }
            return;
        };

        next unless $cmd_result;

        # XXX maybe refacotr the result munging and merging
        my $r = MongoDB::WriteResult->parse(
            op       => $type,
            op_count => scalar @$chunk,
            result   => $cmd_result,
        );

        # append corresponding ops to errors
        if ( $r->count_writeErrors ) {
            for my $error ( @{ $r->writeErrors } ) {
                $error->{op} = $chunk->[ $error->{index} ];
                # convert boolean::true|false back to 1 or 0
                for my $k (qw/upsert multi/) {
                    $error->{op}{$k} = 0+ $error->{op}{$k} if exists $error->{op}{$k};
                }
            }
        }

        $result->merge_result($r);
        $self->_assert_no_error($result) if $ordered;
    }

    return;
}

sub _split_chunk {
    my ( $self, $chunk, $size ) = @_;

    # XXX this call chain is gross; eventually, client (or node) should probably be
    # an attribute of Bulk
    my $max_wire_size = $self->collection->_database->_client->_max_bson_wire_size;

    my $avg_cmd_size       = $size / @$chunk;
    my $new_cmds_per_chunk = int( $max_wire_size / $avg_cmd_size );

    my @split_chunks;
    while (@$chunk) {
        push @split_chunks, [ splice( @$chunk, 0, $new_cmds_per_chunk ) ];
    }

    return @split_chunks;
}

sub _batch_ordered {
    my ($self) = @_;
    my @batches;
    my $last_type = '';
    my $count     = 0;

    # XXX this call chain is gross; eventually, client (or node) should probably be
    # an attribute of Bulk
    my $max_batch_count = $self->collection->_database->_client->_max_write_batch_size;

    for my $op ( $self->_all_ops ) {
        my ( $type, $doc ) = @$op;
        if ( $type ne $last_type || $count == 1000 ) {
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
    my ($self) = @_;
    my %batches = map { ; $_ => [ [] ] } keys %OP_MAP;

    # XXX this call chain is gross; eventually, client (or node) should probably be
    # an attribute of Bulk
    my $max_batch_count = $self->collection->_database->_client->_max_write_batch_size;

    for my $op ( $self->_all_ops ) {
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

sub _assert_no_error {
    my ( $self, $result ) = @_;
    my $error_cnt = $result->count_writeErrors;
    return unless $error_cnt;
    MongoDB::BulkWriteError->throw(
        message => "writeErrors: $error_cnt",
        details => $result,
    );
}

# XXX the _execute_legacy_(insert|update|delete) commands duplicate code in
# Collection.pm, but that code is wrapped up in a way that doesn't easily allow
# grabbing result details.  We can't parse a response directly because the
# network receive code is tightly coupled to a cursor.  These functions work
# around these limitations for the time being

sub _execute_legacy_batch {
    my ( $self, $batch, $result, $ordered ) = @_;
    my ( $type, $docs ) = @$batch;

    my $coll   = $self->collection;
    my $client = $coll->_database->_client;
    my $ns     = $coll->full_name;
    my $method = "_gen_legacy_$type";

    for my $doc (@$docs) {
        # legacy server doesn't check keys on insert
        if ( $type eq 'insert' && ( my $r = $self->_check_no_dollar_keys($doc) ) ) {
            $result->merge_result($r);
            $self->_assert_no_error($result) if $ordered;
            next;
        }

        my $op_string = $self->$method( $ns, $doc );

        if ( length($op_string) > $client->max_bson_size ) {
            # XXX do something here: croak? fake up a result? send anyway and get
            # server response?
        }

        # XXX for bulk, do we just send and ignore errors when not safe?
        my $op_result;
        if ( $client->_w_want_safe ) {
            $op_result = $coll->_make_safe_cursor($op_string)->next;
        }
        else {
            # XXX they don't want safe -- now what?
            # $client->send($op_string);
        }

        # XXX do something with $op_result
        my $gle_result = $self->_get_writeresult_from_gle( $type, $op_result, $doc );

        $result->merge_result($gle_result);
        $self->_assert_no_error($result) if $ordered;
    }

    return;
}

sub _get_writeresult_from_gle {
    my ( $self, $type, $gle, $doc ) = @_;
    my ( @writeErrors, @writeConcernErrors, @upserted );

    # Still checking for $err here because it's not yet handled during
    # reply unpacking
    if ( exists $gle->{'$err'} ) {
        MongoDB::DatabaseError->throw(
            message => $gle->{'$err'},
            details => MongoDB::CommandResult->new( result => $gle ),
        );
    }

    # 'ok' false means GLE itself failed
    if ( !$gle->{ok} ) {
        MongoDB::DatabaseError->throw(
            message => $gle->{errmsg},
            details => MongoDB::CommandResult->new( result => $gle ),
        );
    }

    my $affected = 0;
    my $errmsg =
        defined $gle->{err}    ? $gle->{err}
      : defined $gle->{errmsg} ? $gle->{errmsg}
      :                          undef;

    if ( my $wtimeout = $gle->{wtimeout} ) {
        my $code = $gle->{code} || WRITE_CONCERN_ERROR;
        push @writeConcernErrors,
          {
            errmsg  => $errmsg,
            errInfo => { wtimeout => $wtimeout },
            code    => $code
          };
    }
    elsif ( defined $errmsg ) {
        my $code = $gle->{code} || UNKNOWN_ERROR;
        # index is always 0 because ops are executed individually; later
        # merging of results will fix up the index values as usual
        my $error_doc = {
            errmsg => $errmsg,
            code   => $code,
            index  => 0,
            op     => $doc,
        };
        # convert boolean::true|false back to 1 or 0
        for my $k (qw/upsert multi/) {
            $error_doc->{op}{$k} = 0+ $error_doc->{op}{$k} if exists $error_doc->{op}{$k};
        }
        $error_doc->{errInfo} = $gle->{errInfo} if exists $gle->{errInfo};
        push @writeErrors, $error_doc;
    }
    else {
        # GLE: n only returned for update/remove, so we infer it for insert
        $affected =
            $type eq 'insert' ? 1
          : defined $gle->{n} ? $gle->{n}
          :                     0;

        # index is always 0 because ops are executed individually; later
        # merging of results will fix up the index values as usual
        push @upserted, { index => 0, _id => $gle->{upserted} } if $gle->{upserted};
    }

    my $result = MongoDB::WriteResult->parse(
        op       => $type,
        op_count => 1,
        result   => {
            n                  => $affected,
            writeErrors        => \@writeErrors,
            writeConcernErrors => \@writeConcernErrors,
            ( @upserted ? ( upserted => \@upserted ) : () ),
        },
    );

    return $result;
}

sub _gen_legacy_insert {
    my ( $self, $ns, $doc ) = @_;
    # $doc is a document to insert

    # for bulk, we don't accumulate IDs
    my ( $insert, undef ) = MongoDB::write_insert( $ns, [$doc], 0 );

    return $insert;
}

sub _gen_legacy_update {
    my ( $self, $ns, $doc ) = @_;
    # $doc is { q: $query, u: $update, multi: $multi, upsert: $upsert }

    my $flags = 0;
    $flags |= 1 << 0 if $doc->{upsert};
    $flags |= 1 << 1 if $doc->{multi};

    return MongoDB::write_update( $ns, $doc->{q}, $doc->{u}, $flags );
}

sub _gen_legacy_delete {
    my ( $self, $ns, $doc ) = @_;
    # $doc is { q: $query, limit: $limit }

    return MongoDB::write_remove( $ns, $doc->{q}, $doc->{limit} ? 1 : 0 );
}

sub _check_no_dollar_keys {
    my ( $self, $doc ) = @_;

    if ( my @bad = grep { substr( $_, 0, 1 ) eq '$' } keys %$doc ) {
        my $errdoc = {
            index  => 0,
            errmsg => "Document can't have '\$' prefixed field names: @bad",
            code   => UNKNOWN_ERROR
        };

        return MongoDB::WriteResult->new(
            op_count    => 1,
            writeErrors => [$errdoc]
        );
    }

    return;
}

__PACKAGE__->meta->make_immutable;

1;
