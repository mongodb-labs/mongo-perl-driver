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

has '_wire_version' => (
    is         => 'ro',
    isa        => 'Int',
    lazy_build => 1,
);

sub _build__wire_version {
    my ($self) = @_;
    return $self->collection->_database->_client->max_wire_version;
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
        if ( $self->_wire_version > 1 ) {
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

sub _execute_legacy_batch {
    die "unimplemented"
}

__PACKAGE__->meta->make_immutable;

1;
