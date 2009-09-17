#
#  Copyright 2009 10gen, Inc.
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

package MongoDB::Cursor;
# ABSTRACT: A cursor/iterator for Mongo query results

use Any::Moose;

=head1 SYNOPSIS

    while (my $object = $cursor->next) {
        ...
    }

    my @objects = $cursor->all;

=cut

has _oid_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'MongoDB::OID',
);


$MongoDB::Cursor::slave_okay = 0;


=method fields

    $coll->insert({name => "Fred", age => 20});
    my $cursor = $coll->find->fields({ name => 1 });
    my $obj = $cursor->next;
    $obj->{name}; "Fred"
    $obj->{age}; # undef

Selects which fields are returned. 
The default is all fields.  _id is always returned.

=method sort

    # sort by name, descending
    my $sort = {"name" => -1};
    $cursor = $coll->find->sort($sort);

Adds a sort to the query.
Returns this cursor for chaining operations.


=method limit

    $per_page = 20;
    $cursor = $coll->find->limit($per_page);

Returns a maximum of N results.
Returns this cursor for chaining operations.


=method skip

    $page_num = 7;
    $per_page = 100;
    $cursor = $coll->find->limit($per_page)->skip($page_num * $per_page);

Skips the first N results.
Returns this cursor for chaining operations.


=method snapshot

    my $cursor = $coll->find->snapshot;

Uses snapshot mode for the query.  Snapshot mode assures no 
duplicates are returned, or objects missed, which were present 
at both the start and end of the query's execution (if an object 
is new during the query, or deleted during the query, it may or 
may not be returned, even with snapshot mode).  Note that short 
query responses (less than 1MB) are always effectively 
snapshotted.  Currently, snapshot mode may not be used with 
sorting or explicit hints.


=method hint

    my $cursor = $coll->find->hint({'x' => 1});

Force Mongo to use a specific index for a query.


=method explain

    my $explanation = $cursor->explain;

This will tell you the type of cursor used, the number of records 
the DB had to examine as part of this query, the number of records 
returned by the query, and the time in milliseconds the query took 
to execute.


=method reset

Resets the cursor.  After being reset, pre-query methods can be
called on the cursor (sort, limit, etc.) and subsequent calls to
next, has_next, or all will re-query the database.


=method has_next

    while ($cursor->has_next) {
        ...
    }

Checks if there is another result to fetch.


=method next

    while (my $object = $cursor->next) {
        ...
    }

Returns the next object in the cursor. Will automatically fetch more data from
the server if necessary. Returns undef if no more data is available.


=method all

    my @objects = $cursor->all;

Returns a list of all objects in the result.

=cut

sub all {
    my ($self) = @_;
    my @ret;

    while (my $entry = $self->next) {
        push @ret, $entry;
    }

    return @ret;
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
