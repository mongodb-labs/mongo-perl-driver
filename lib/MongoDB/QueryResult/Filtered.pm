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

package MongoDB::QueryResult::Filtered;

# ABSTRACT: An iterator for Mongo query results with client-side filtering

use version;
our $VERSION = 'v0.999.999.3'; # TRIAL

use Moose;
use Types::Standard -types;

extends 'MongoDB::QueryResult';

use namespace::clean -except => 'meta';

has post_filter => (
    is       => 'ro',
    isa      => CodeRef,
    required => 1,
);

sub has_next {
    my ($self) = @_;
    my $limit = $self->limit;
    if ( $limit > 0 && ( $self->cursor_at + 1 ) > $limit ) {
        $self->_kill_cursor;
        return 0;
    }
    while ( !$self->_drained || $self->_get_more ) {
        my $peek = $self->_docs->[0];
        if ( $self->post_filter->($peek) ) {
            # if meets criteria, has_next is true
            return 1;
        }
        else {
            # otherwise throw it away and repeat
            $self->_inc_cursor_at;
            $self->_next_doc;
        }
    }
    # ran out of docs, so nothing left
    return 0;
}

__PACKAGE__->meta->make_immutable;

1;

=for Pod::Coverage
has_next

=cut
