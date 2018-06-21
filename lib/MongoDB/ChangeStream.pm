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
package MongoDB::ChangeStream;

# ABSTRACT: A stream providing update information for collections.

use version;
our $VERSION = 'v1.999.1';

use Moo;
use Try::Tiny;
use MongoDB::Cursor;
use MongoDB::Op::_ChangeStream;
use MongoDB::Error;
use Safe::Isa;
use BSON::Timestamp;
use MongoDB::_Types qw(
    MongoDBCollection
    ArrayOfHashRef
    Boolish
    Intish
    BSONTimestamp
    ClientSession
);
use Types::Standard qw(
    InstanceOf
    HashRef
    Maybe
    Str
    Num
);

use namespace::clean -except => 'meta';

has _result => (
    is => 'rw',
    isa => InstanceOf['MongoDB::QueryResult'],
    init_arg => undef,
    lazy => 1,
    builder => '_build_result',
    clearer => '_clear_result',
);

has _client => (
    is => 'ro',
    isa => InstanceOf['MongoDB::MongoClient'],
    init_arg => 'client',
    required => 1,
);

has _op_args => (
    is => 'ro',
    isa => HashRef,
    init_arg => 'op_args',
    required => 1,
);

has _pipeline => (
    is => 'ro',
    isa => ArrayOfHashRef,
    init_arg => 'pipeline',
    required => 1,
);

has _full_document => (
    is => 'ro',
    isa => Str,
    init_arg => 'full_document',
    predicate => '_has_full_document',
);

has _resume_token => (
    is => 'rw',
    init_arg => 'resume_after',
    predicate => '_has_resume_token',
);

has _all_changes_for_cluster => (
    is => 'ro',
    isa => Boolish,
    init_arg => 'all_changes_for_cluster',
    default => sub { 0 },
);

has _changes_received => (
    is => 'rw',
    isa => Boolish,
    init_arg => 'changes_received',
    default => sub { 0 },
);

has _start_at_operation_time => (
    is => 'ro',
    isa => BSONTimestamp,
    init_arg => 'start_at_operation_time',
    predicate => '_has_start_at_operation_time',
    coerce => sub {
        ref($_[0]) ? $_[0] : BSON::Timestamp->new($_[0])
    },
);

has _session => (
    is => 'ro',
    isa => Maybe[ClientSession],
    init_arg => 'session',
);

has _options => (
    is => 'ro',
    isa => HashRef,
    init_arg => 'options',
    default => sub { {} },
);

has _max_await_time_ms => (
    is => 'ro',
    isa => Num,
    init_arg => 'max_await_time_ms',
    predicate => '_has_max_await_time_ms',
);

sub BUILD {
    my ($self) = @_;

    # starting point is construction time instead of first next call
    $self->_result;
}

sub _build_result {
    my ($self) = @_;

    my $op = MongoDB::Op::_ChangeStream->new(
        pipeline => $self->_pipeline,
        all_changes_for_cluster => $self->_all_changes_for_cluster,
        changes_received => $self->_changes_received,
        session => $self->_session,
        options => $self->_options,
        client => $self->_client,
        $self->_has_start_at_operation_time
            ? (start_at_operation_time => $self->_start_at_operation_time)
            : (),
        $self->_has_full_document
            ? (full_document => $self->_full_document)
            : (),
        $self->_has_resume_token
            ? (resume_after => $self->_resume_token)
            : (),
        $self->_has_max_await_time_ms
            ? (maxAwaitTimeMS => $self->_max_await_time_ms)
            : (),
        %{ $self->_op_args },
    );

    return $self->_client->send_read_op($op);
}

=head1 STREAM METHODS

=cut

=head2 next

    $change_stream = $collection->watch(...);
    $change = $change_stream->next;

Waits for the next change in the collection and returns it.

B<Note>: This method will wait for the amount of milliseconds passed
as C<maxAwaitTimeMS> to L<MongoDB::Collection/watch> or the server's
default wait-time. It will not wait indefinitely.

=cut

sub next {
    my ($self) = @_;

    my $change;
    my $retried;
    while (1) {
        last if try {
            $change = $self->_result->next;
            1 # successfully fetched result
        }
        catch {
            my $error = $_;
            if (
                not($retried)
                and $error->$_isa('MongoDB::Error')
                and $error->_is_resumable
            ) {
                $retried = 1;
                $self->_result($self->_build_result);
            }
            else {
                die $error;
            }
            0 # failed, cursor was rebuilt
        };
    }

    # this differs from drivers that block indefinitely. we have to
    # deal with the situation where no results are available.
    if (not defined $change) {
        return undef; ## no critic
    }

    if (exists $change->{_id}) {
        $self->_resume_token($change->{_id});
        $self->_changes_received(1);
        return $change;
    }
    else {
        MongoDB::InvalidOperationError->throw(
            "Cannot provide resume functionality when the ".
            "resume token is missing");
    }
}

1;

=head1 SYNOPSIS

    $stream = $collection->watch( $pipeline, $options );
    while(1) {

        # This inner loop will only iterate until there are no more
        # changes available.
        while (my $change = $stream->next) {
            ...
        }
    }

=head1 DESCRIPTION

This class models change stream results as returned by the
L<MongoDB::Collection/watch> method.

=head1 SEE ALSO

The L<Change Streams manual section|https://docs.mongodb.com/manual/changeStreams/>.

The L<Change Streams specification|https://github.com/mongodb/specifications/blob/master/source/change-streams.rst>.

=cut
