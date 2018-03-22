#
#  Copyright 2009-2018 MongoDB, Inc.
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

use strict;
use warnings;
package MongoDB::ChangeStream;

# ABSTRACT: A stream providing update information for collections.

use Moo;
use Try::Tiny;
use MongoDB::Cursor;
use MongoDB::Op::_Aggregate;
use MongoDB::Error;

use namespace::clean -except => 'meta';

has _cursor => (
    is => 'rw',
    lazy => 1,
    builder => '_build_cursor',
    clearer => '_clear_cursor',
);

has collection => (
    is => 'ro',
    required => 1,
);

has options => (
    is => 'ro',
);

has pipeline => (
    is => 'ro',
    required => 1,
);

has full_document => (
    is => 'ro',
    predicate => '_has_full_document',
);

has _resume_token => (
    is => 'rw',
    init_arg => 'resume_after',
    predicate => '_has_resume_token',
    lazy => 1,
    builder => '_build_resume_token',
);

sub BUILD {
    my ($self) = @_;

    # starting point is construction time instead of first next call
    $self->_cursor;
}

sub _build_cursor {
    my ($self) = @_;

    my $pipeline = $self->pipeline;

    my @pipeline = @{ $self->pipeline || [] };
    @pipeline = (
        {'$changeStream' => {
            ($self->_has_full_document
                ? (fullDocument => $self->full_document)
                : ()
            ),
            ($self->_has_resume_token
                ? (resumeAfter => $self->_resume_token)
                : ()
            ),
        }},
        @pipeline,
    );

    return $self->collection->aggregate(
        \@pipeline,
        {
            %{ $self->options || {} },
            cursorType => 'tailable_await',
        },
    );
}

=head1 STREAM METHODS

=cut

=head2 next

    $change_stream = $collection->watch(...);
    $change = $change_stream->next;

Waits for the next change in the collection and returns it.

B<Note>: This method will wait for the amount of milliseconds passed
as C<maxAwaitTimeMS> o L<MongoDB::Collection/watch> or the default. It
will not wait indefinitely.

=cut

sub next {
    my ($self) = @_;

    my $change;
    while (1) {
        my $success = try {
            $change = $self->_cursor->next;
            1 # successfully fetched result
        }
        catch {
            my $error = $_;
            if (
                $error->isa('MongoDB::ConnectionError')
                or
                $error->isa('MongoDB::CursorNotFoundError')
            ) {
                $self->_cursor($self->_build_cursor);
            }
            else {
                die $error;
            }
            0 # failed, cursor was rebuilt
        };
        last if $success;
    }

    # this differs from drivers that block indefinitely. we have to
    # deal with the situation where no results are available.
    if (not defined $change) {
        return undef;
    }

    if (exists $change->{_id}) {
        my $resume_token = $change->{_id};
        $self->_resume_token($resume_token);
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
    while (my $change = $stream->next) {
        ...
    }

=head1 DESCRIPTION

This class models change stream results as returned by the
L<MongoDB::Collection/watch> method.

=head1 SEE ALSO

The L<Change Streams manual section|https://docs.mongodb.com/manual/changeStreams/>.

The L<Change Streams specification|https://github.com/mongodb/specifications/blob/master/source/change-streams.rst>.

=cut
