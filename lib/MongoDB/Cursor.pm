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

package MongoDB::Cursor;


# ABSTRACT: A cursor/iterator for Mongo query results
use Moose;
use boolean;
use Tie::IxHash;

=head1 NAME

MongoDB::Cursor - A cursor/iterator for Mongo query results

=head1 SYNOPSIS

    while (my $object = $cursor->next) {
        ...
    }

    my @objects = $cursor->all;

=head2 Multithreading

Cloning instances of this class is disabled in Perl 5.8.7+, so forked threads
will have to create their own database queries.

=head1 SEE ALSO

Core documentation on cursors: L<http://dochub.mongodb.org/core/cursors>.

=cut

$MongoDB::Cursor::_request_id = int(rand(1000000));

=head1 STATIC ATTRIBUTES

=head2 slave_okay

    $MongoDB::Cursor::slave_okay = 1;

Whether it is okay to run queries on the slave.  Defaults to 0.

=cut

$MongoDB::Cursor::slave_okay = 0;

=head2 timeout

B<Deprecated, use MongoDB::Connection::query_timeout instead.>

How many milliseconds to wait for a response from the server.  Set to 30000
(30 seconds) by default.  -1 waits forever (or until TCP times out, which is
usually a long time).

This value is overridden by C<MongoDB::Connection::query_timeout> and never
used.

=cut

$MongoDB::Cursor::timeout = 30000;

=head1 ATTRIBUTES

=head2 started_iterating

If this cursor has queried the database yet. Methods
mofifying the query will complain if they are called
after the database is queried.

=cut

has started_iterating => (
    is => 'rw',
    isa => 'Bool',
    required => 1,
    default => 0,
);

has _client => (
    is => 'ro',
    isa => 'MongoDB::MongoClient',
    required => 1,
);

has _ns => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has _query => (
    is => 'rw',
    required => 1,
);

has _fields => (
    is => 'rw',
    required => 0,
);

has _limit => (
    is => 'rw',
    isa => 'Int',
    required => 0,
    default => 0,
);

has _skip => (
    is => 'rw',
    isa => 'Int',
    required => 0,
    default => 0,
);

has _tailable => (
    is => 'rw',
    isa => 'Bool',
    required => 0,
    default => 0,
);




=head2 immortal

    $cursor->immortal(1);

Ordinarily, a cursor "dies" on the database server after a certain length of
time (approximately 10 minutes), to prevent inactive cursors from hogging
resources.  This option sets that a cursor should not die until all of its
results have been fetched or it goes out of scope in Perl.

Boolean value, defaults to 0.

C<immortal> is not equivalent to setting a client-side timeout.  If you are
getting client-side timeouts (e.g., "recv timed out"), set C<query_timeout> on
your connection.

    # wait forever for a query to return results
    $connection->query_timeout(-1);

See L<MongoDB::Connection/query_timeout>.

=cut


has immortal => (
    is => 'rw',
    isa => 'Bool',
    required => 0,
    default => 0,
);


=head2 partial

If a shard is down, mongos will return an error when it tries to query that
shard.  If this is set, mongos will just skip that shard, instead.

Boolean value, defaults to 0.

=cut

has partial => (
    is => 'rw',
    isa => 'Bool',
    required => 0,
    default => 0,
);

=head2 slave_okay

    $cursor->slave_okay(1);

If a query can be done on a slave database server.

Boolean value, defaults to 0.

=cut

has slave_okay => (
    is => 'rw',
    isa => 'Bool',
    required => 0,
    default => 0,
);


# stupid hack for inconsistent database handling of queries
has _grrrr => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0,
);

has _request_id => (
    is      => 'rw',
    isa     => 'Int',
    default => 0,
);

=head1 METHODS

=cut

sub _ensure_special {
    my ($self) = @_;

    if ($self->_grrrr) {
        return;
    }

    $self->_grrrr(1);
    $self->_query({'query' => $self->_query})
}

# this does the query if it hasn't been done yet
sub _do_query {
    my ($self) = @_;

    if ($self->started_iterating) {
        return;
    }

    my $opts = ($self->_tailable() << 1) |
        (($MongoDB::Cursor::slave_okay | $self->slave_okay) << 2) |
        ($self->immortal << 4) |
        ($self->partial << 7);

    my ($query, $info) = MongoDB::write_query($self->_ns, $opts, $self->_skip, $self->_limit, $self->_query, $self->_fields);
    $self->_request_id($info->{'request_id'});

    $self->_client->send($query);
    $self->_client->recv($self);

    $self->started_iterating(1);
}

=head2 fields (\%f)

    $coll->insert({name => "Fred", age => 20});
    my $cursor = $coll->query->fields({ name => 1 });
    my $obj = $cursor->next;
    $obj->{name}; "Fred"
    $obj->{age}; # undef

Selects which fields are returned.
The default is all fields.  _id is always returned.

=cut

sub fields {
    my ($self, $f) = @_;
    confess "cannot set fields after querying"
	if $self->started_iterating;
    confess 'not a hash reference'
	    unless ref $f eq 'HASH' || ref $f eq 'Tie::IxHash';

    $self->_fields($f);
    return $self;
}

=head2 sort ($order)

    # sort by name, descending
    my $sort = {"name" => -1};
    $cursor = $coll->query->sort($sort);

Adds a sort to the query.  Argument is either
a hash reference or a Tie::IxHash.
Returns this cursor for chaining operations.

=cut

sub sort {
    my ($self, $order) = @_;
    confess "cannot set sort after querying"
	if $self->started_iterating;
    confess 'not a hash reference'
	    unless ref $order eq 'HASH' || ref $order eq 'Tie::IxHash';

    $self->_ensure_special;
    $self->_query->{'orderby'} = $order;
    return $self;
}


=head2 limit ($num)

    $per_page = 20;
    $cursor = $coll->query->limit($per_page);

Returns a maximum of N results.
Returns this cursor for chaining operations.

=cut

sub limit {
    my ($self, $num) = @_;
    confess "cannot set limit after querying"
	if $self->started_iterating;

    $self->_limit($num);
    return $self;
}


=head2 tailable ($bool)

    $cursor->query->tailable(1);

If a cursor should be tailable.  Tailable cursors can only be used on capped
collections and are similar to the C<tail -f> command: they never die and keep
returning new results as more is added to a collection.

They are often used for getting log messages.

Boolean value, defaults to 0.

Returns this cursor for chaining operations.

=cut

sub tailable {
	my($self, $bool) = @_;
	confess "cannot set tailable after querying"
	if $self->started_iterating;
	
	$self->_tailable($bool);
	return $self;
}



=head2 skip ($num)

    $page_num = 7;
    $per_page = 100;
    $cursor = $coll->query->limit($per_page)->skip($page_num * $per_page);

Skips the first N results. Returns this cursor for chaining operations.

See also core documentation on limit: L<http://dochub.mongodb.org/core/limit>.

=cut

sub skip {
    my ($self, $num) = @_;
    confess "cannot set skip after querying"
	if $self->started_iterating;

    $self->_skip($num);
    return $self;
}

=head2 snapshot

    my $cursor = $coll->query->snapshot;

Uses snapshot mode for the query.  Snapshot mode assures no
duplicates are returned, or objects missed, which were present
at both the start and end of the query's execution (if an object
is new during the query, or deleted during the query, it may or
may not be returned, even with snapshot mode).  Note that short
query responses (less than 1MB) are always effectively
snapshotted.  Currently, snapshot mode may not be used with
sorting or explicit hints.

=cut

sub snapshot {
    my ($self) = @_;
    confess "cannot set snapshot after querying"
	if $self->started_iterating;

    $self->_ensure_special;
    $self->_query->{'$snapshot'} = 1;
    return $self;
}

=head2 hint

    my $cursor = $coll->query->hint({'x' => 1});

Force Mongo to use a specific index for a query.

=cut

sub hint {
    my ($self, $index) = @_;
    confess "cannot set hint after querying"
	if $self->started_iterating;
    confess 'not a hash reference'
    	unless ref $index eq 'HASH' || ref $index eq 'Tie::IxHash';

    $self->_ensure_special;
    $self->_query->{'$hint'} = $index;
    return $self;
}

=head2 explain

    my $explanation = $cursor->explain;

This will tell you the type of cursor used, the number of records the DB had to
examine as part of this query, the number of records returned by the query, and
the time in milliseconds the query took to execute.  Requires L<boolean> package.

C<explain> resets the cursor, so calling C<next> or C<has_next> after an explain
will requery the database.

See also core documentation on explain:
L<http://dochub.mongodb.org/core/explain>.

=cut

sub explain {
    my ($self) = @_;
    my $temp = $self->_limit;
    if ($self->_limit > 0) {
        $self->_limit($self->_limit * -1);
    }

    $self->_ensure_special;
    $self->_query->{'$explain'} = boolean::true;

    my $retval = $self->reset->next;
    $self->reset->limit($temp);

    undef $self->_query->{'$explain'};

    return $retval;
}

=head2 count($all?)

    my $num = $cursor->count;
    my $num = $cursor->skip(20)->count(1);

Returns the number of document this query will return.  Optionally takes a
boolean parameter, indicating that the cursor's limit and skip fields should be
used in calculating the count.

=cut

sub count {
    my ($self, $all) = @_;

    my ($db, $coll) = $self->_ns =~ m/^([^\.]+)\.(.*)/;
    my $cmd = new Tie::IxHash(count => $coll);

    if ($self->_grrrr) {
        $cmd->Push(query => $self->_query->{'query'});
    }
    else {
        $cmd->Push(query => $self->_query);
    }

    if ($all) {
        $cmd->Push(limit => $self->_limit) if $self->_limit;
        $cmd->Push(skip => $self->_skip) if $self->_skip;
    }

    my $result = $self->_client->get_database($db)->run_command($cmd);

    # returns "ns missing" if collection doesn't exist
    return 0 unless ref $result eq 'HASH';
    return $result->{'n'};
}


# shortcut to make some XS code saner
sub _dt_type { 
    my $self = shift;
    return $self->_client->dt_type;
}

sub _inflate_dbrefs {
    my $self = shift;
    return $self->_client->inflate_dbrefs;
}


=head2 reset

Resets the cursor.  After being reset, pre-query methods can be
called on the cursor (sort, limit, etc.) and subsequent calls to
next, has_next, or all will re-query the database.


=head2 has_next

    while ($cursor->has_next) {
        ...
    }

Checks if there is another result to fetch.


=head2 next

    while (my $object = $cursor->next) {
        ...
    }

Returns the next object in the cursor. Will automatically fetch more data from
the server if necessary. Returns undef if no more data is available.

=head2 info

Returns a hash of information about this cursor.  Currently the fields are:

=over 4

=item C<cursor_id>

The server-side id for this cursor.  A C<cursor_id> of 0 means that there are no
more batches to be fetched.

=item C<num>

The number of results returned so far.

=item C<at>

The index of the result the cursor is currently at.

=item C<flag>

If the database could not find the cursor or another error occurred, C<flag> may
be set (depending on the error).
See L<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPREPLY>
for a full list of flag values.

=item C<start>

The index of the result that the current batch of results starts at.

=back

=head2 all

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


__PACKAGE__->meta->make_immutable (inline_destructor => 0);

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
