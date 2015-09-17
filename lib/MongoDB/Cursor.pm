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


# ABSTRACT: A lazy cursor for Mongo query results

use version;
our $VERSION = 'v1.1.0';

use Moo;
use MongoDB;
use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::QueryResult;
use MongoDB::ReadPreference;
use MongoDB::_Protocol;
use MongoDB::Op::_Explain;
use MongoDB::_Types -types, 'to_IxHash';
use Types::Standard qw(
    InstanceOf
);
use boolean;
use Tie::IxHash;
use Try::Tiny;
use namespace::clean -except => 'meta';

=attr started_iterating

A boolean indicating if this cursor has queried the database yet. Methods
modifying the query will complain if they are called after the database is
queried.

=cut

with 'MongoDB::Role::_Cursor';

# attributes for sending a query
has query => (
    is => 'ro',
    isa => InstanceOf['MongoDB::_Query'],
    required => 1,
);

# lazy result attribute
has result => (
    is        => 'lazy',
    isa       => InstanceOf['MongoDB::QueryResult'],
    builder   => '_build_result',
    predicate => 'started_iterating',
    clearer   => '_clear_result',
);

# this does the query if it hasn't been done yet
sub _build_result {
    my ($self) = @_;
    $self->query->execute;
}

#--------------------------------------------------------------------------#
# methods that modify the query
#--------------------------------------------------------------------------#

=head1 QUERY MODIFIERS

These methods modify the query to be run.  An exception will be thrown if
they are called after results are iterated.

=head2 immortal

    $cursor->immortal(1);

Ordinarily, a cursor "dies" on the database server after a certain length of
time (approximately 10 minutes), to prevent inactive cursors from hogging
resources.  This option indicates that a cursor should not die until all of its
results have been fetched or it goes out of scope in Perl.

Boolean value, defaults to 0.

Note: C<immortal> only affects the server-side timeout.  If you are getting
client-side timeouts you will need to change your client configuration.
See L<MongoDB::MongoClient/max_time_ms> and
L<MongoDB::MongoClient/socket_timeout_ms>.

Returns this cursor for chaining operations.

=cut

sub immortal {
    my ( $self, $bool ) = @_;
    MongoDB::UsageError->throw("cannot set immortal after querying")
        if $self->started_iterating;

    $self->query->noCursorTimeout(!!$bool);
    return $self;
}

=head2 fields

    $coll->insert({name => "Fred", age => 20});
    my $cursor = $coll->find->fields({ name => 1 });
    my $obj = $cursor->next;
    $obj->{name}; "Fred"
    $obj->{age}; # undef

Selects which fields are returned.  The default is all fields.  When fields
are specified, _id is returned by default, but this can be disabled by
explicitly setting it to "0".  E.g.  C<< _id => 0 >>. Argument must be either a
hash reference or a L<Tie::IxHash> object.

See L<Limit fields to
return|http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/>
in the MongoDB documentation for details.

Returns this cursor for chaining operations.

=cut

sub fields {
    my ($self, $f) = @_;
    MongoDB::UsageError->throw("cannot set fields after querying")
      if $self->started_iterating;
    MongoDB::UsageError->throw("not a hash reference")
      unless ref $f eq 'HASH' || ref $f eq 'Tie::IxHash';

    $self->query->projection($f);
    return $self;
}

=head2 sort

    # sort by name, descending
    $cursor->sort([name => -1]);

Adds a sort to the query.  Argument is either a hash reference or a
L<Tie::IxHash> or an array reference of key/value pairs.  Because hash
references are not ordered, do not use them for more than one key.

Returns this cursor for chaining operations.

=cut

sub sort {
    my ( $self, $order ) = @_;
    MongoDB::UsageError->throw("cannot set sort after querying")
      if $self->started_iterating;

    $self->query->sort( to_IxHash($order) );
    return $self;
}


=head2 limit

    $cursor->limit(20);

Sets cursor to return a maximum of N results.

Returns this cursor for chaining operations.

=cut

sub limit {
    my ( $self, $num ) = @_;
    MongoDB::UsageError->throw("cannot set limit after querying")
      if $self->started_iterating;
    $self->query->limit($num);
    return $self;
}


=head2 max_time_ms

    $cursor->max_time_ms( 500 );

Causes the server to abort the operation if the specified time in milliseconds
is exceeded.

Returns this cursor for chaining operations.

=cut

sub max_time_ms {
    my ( $self, $num ) = @_;
    $num = 0 unless defined $num;
    MongoDB::UsageError->throw("max_time_ms must be non-negative")
      if $num < 0;
    MongoDB::UsageError->throw("can not set max_time_ms after querying")
      if $self->started_iterating;

    $self->query->maxTimeMS( $num );
    return $self;

}

=head2 tailable

    $cursor->tailable(1);

If a cursor should be tailable.  Tailable cursors can only be used on capped
collections and are similar to the C<tail -f> command: they never die and keep
returning new results as more is added to a collection.

They are often used for getting log messages.

Boolean value, defaults to 0.

If you want the tailable cursor to block for a few seconds, use
L</tailable_await> instead.  B<Note> calling this with a false value
disables tailing, even if C<tailable_await> was previously called.

Returns this cursor for chaining operations.

=cut

sub tailable {
    my ( $self, $bool ) = @_;
    MongoDB::UsageError->throw("cannot set tailable after querying")
        if $self->started_iterating;

    $self->query->cursorType($bool ? 'tailable' : 'non_tailable');
    return $self;
}

=head2 tailable_await

    $cursor->tailable_await(1);

Sets a cursor to be tailable and block for a few seconds if no data
is immediately available.

Boolean value, defaults to 0.

If you want the tailable cursor without blocking, use L</tailable> instead.
B<Note> calling this with a false value disables tailing, even if C<tailable>
was previously called.

=cut

sub tailable_await {
    my ( $self, $bool ) = @_;
    MongoDB::UsageError->throw("cannot set tailable_await after querying")
        if $self->started_iterating;

    $self->query->cursorType($bool ? 'tailable_await' : 'non_tailable');
    return $self;
}

=head2 skip

    $cursor->skip( 50 );

Skips the first N results.

Returns this cursor for chaining operations.

=cut

sub skip {
    my ( $self, $num ) = @_;
    MongoDB::UsageError->throw("skip must be non-negative")
      if $num < 0;
    MongoDB::UsageError->throw("cannot set skip after querying")
      if $self->started_iterating;

    $self->query->skip($num);
    return $self;
}

=head2 snapshot

    $cursor->snapshot(1);

Uses snapshot mode for the query.  Snapshot mode assures no duplicates are
returned due an intervening write relocating a document.  Note that if an
object is inserted, updated or deleted during the query, it may or may not
be returned when snapshot mode is enabled. Short query responses (less than
1MB) are always effectively snapshotted.  Currently, snapshot mode may not
be used with sorting or explicit hints.

Returns this cursor for chaining operations.

=cut

sub snapshot {
    my ($self, $bool) = @_;

    MongoDB::UsageError->throw("cannot set snapshot after querying")
      if $self->started_iterating;

    MongoDB::UsageError->throw("snapshot requires a defined, boolean argument")
      unless defined $bool;

    $self->query->modifiers->{'$snapshot'} = $bool;
    return $self;
}

=head2 hint

    $cursor->hint({'x' => 1});
    $cursor->hint(['x', 1]);
    $cursor->hint('x_1');

Force Mongo to use a specific index for a query.

Returns this cursor for chaining operations.

=cut

sub hint {
    my ( $self, $index ) = @_;
    MongoDB::UsageError->throw("cannot set hint after querying")
      if $self->started_iterating;

    # $index must either be a string or a reference to an array, hash, or IxHash
    if ( ref $index eq 'ARRAY' ) {
        $index = Tie::IxHash->new(@$index);
    }
    elsif ( ref $index && !( ref $index eq 'HASH' || ref $index eq 'Tie::IxHash' ) ) {
        MongoDB::UsageError->throw("not a hash reference");
    }

    $self->query->modifiers->{'$hint'} = $index;
    return $self;
}

=head2 partial

    $cursor->partial(1);

If a shard is down, mongos will return an error when it tries to query that
shard.  If this is set, mongos will just skip that shard, instead.

Boolean value, defaults to 0.

Returns this cursor for chaining operations.

=cut

sub partial {
    my ($self, $value) = @_;
    MongoDB::UsageError->throw("cannot set partial after querying")
      if $self->started_iterating;

    $self->query->allowPartialResults( !! $value );

    # returning self is an API change but more consistent with other cursor methods
    return $self;
}

=head2 read_preference

    $cursor->read_preference($read_preference_object);
    $cursor->read_preference('secondary', [{foo => 'bar'}]);

Sets read preference for the cursor's connection.

If given a single argument that is a L<MongoDB::ReadPreference> object, the
read preference is set to that object.  Otherwise, it takes positional
arguments: the read preference mode and a tag set list, which must be a valid
mode and tag set list as described in the L<MongoDB::ReadPreference>
documentation.

Returns this cursor for chaining operations.

=cut

sub read_preference {
    my $self = shift;
    MongoDB::UsageError->throw("cannot set read preference after querying")
      if $self->started_iterating;

    my $type = ref $_[0];
    if ( $type eq 'MongoDB::ReadPreference' ) {
        $self->query->read_preference( $_[0] );
    }
    else {
        my $mode     = shift || 'primary';
        my $tag_sets = shift;
        my $rp       = MongoDB::ReadPreference->new(
            mode => $mode,
            ( $tag_sets ? ( tag_sets => $tag_sets ) : () )
        );
        $self->query->read_preference($rp);
    }

    return $self;
}

=head1 QUERY INTROSPECTION AND RESET

These methods run introspection methods on the query conditions and modifiers
stored within the cursor object.

=head2 explain

    my $explanation = $cursor->explain;

This will tell you the type of cursor used, the number of records the DB had to
examine as part of this query, the number of records returned by the query, and
the time in milliseconds the query took to execute.

See also core documentation on explain:
L<http://dochub.mongodb.org/core/explain>.

=cut

sub explain {
    my ($self) = @_;

    my $explain_op = MongoDB::Op::_Explain->_new(
        db_name         => $self->query->db_name,
        coll_name       => $self->query->coll_name,
        bson_codec      => $self->query->bson_codec,
        query           => $self->query->clone,
        read_preference => $self->query->read_preference
    );

    return $self->query->client->send_read_op($explain_op);
}

=head1 QUERY ITERATION

These methods allow you to iterate over results.

=head2 result

    my $result = $cursor->result;

This method will execute the query and return a L<MongoDB::QueryResult> object
with the results.

The C<has_next>, C<next>, and C<all> methods call C<result> internally,
which executes the query "on demand".

Iterating with a MongoDB::QueryResult object directly instead of a
L<MongoDB::Cursor> will be slightly faster, since the L<MongoDB::Cursor>
methods below just internally call the corresponding method on the result
object.

=cut

#--------------------------------------------------------------------------#
# methods delgated to result object
#--------------------------------------------------------------------------#

=head2 has_next

    while ($cursor->has_next) {
        ...
    }

Checks if there is another result to fetch.  Will automatically fetch more
data from the server if necessary.

=cut

sub has_next { $_[0]->result->has_next }

=head2 next

    while (my $object = $cursor->next) {
        ...
    }

Returns the next object in the cursor. Will automatically fetch more data from
the server if necessary. Returns undef if no more data is available.

=cut

sub next { $_[0]->result->next }

=head2 all

    my @objects = $cursor->all;

Returns a list of all objects in the result.

=cut

sub all { $_[0]->result->all }

=head2 reset

Resets the cursor.  After being reset, pre-query methods can be
called on the cursor (sort, limit, etc.) and subsequent calls to
result, next, has_next, or all will re-query the database.

=cut

sub reset {
    my ($self) = @_;
    $self->_clear_result;
    return $self;
}

=head2 info

Returns a hash of information about this cursor.  This is intended for
debugging purposes and users should not rely on the contents of this method for
production use.  Currently the fields are:

=for :list
* C<cursor_id>  -- the server-side id for this cursor as.  This is an opaque string.
  A C<cursor_id> of "\0\0\0\0\0\0\0\0" means there are no more results on the server.
* C<num> -- the number of results received from the server so far
* C<at> -- the (zero-based) index of the document that will be returned next from L</next>
* C<flag> -- if the database could not find the cursor or another error occurred, C<flag> may
  contain a hash reference of flags set in the response (depending on the error).  See
  L<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol#MongoWireProtocol-OPREPLY>
  for a full list of flag values.
* C<start> -- the index of the result that the current batch of results starts at.

If the cursor has not yet executed, only the C<num> field will be returned with
a value of 0.

=cut

sub info {
    my $self = shift;
    if ( $self->started_iterating ) {
        return $self->result->_info;
    }
    else {
        return { num => 0 };
    }
}

#--------------------------------------------------------------------------#
# Deprecated methods
#--------------------------------------------------------------------------#

sub count {
    my ($self, $limit_skip) = @_;

    my $cmd = new Tie::IxHash(count => $self->query->coll_name);

    $cmd->Push(query => $self->query->filter);

    if ($limit_skip) {
        $cmd->Push(limit => $self->query->limit) if $self->query->limit;
        $cmd->Push(skip => $self->query->skip) if $self->query->skip;
    }

    if (my $hint = $self->query->modifiers->{'$hint'}) {
        $cmd->Push(hint => $hint);
    }

    my $result = try {
        my $db = $self->query->client->get_database( $self->query->db_name );
        $db->run_command( $cmd, $self->query->read_preference );
    }
    catch {
        # if there was an error, check if it was the "ns missing" one that means the
        # collection hasn't been created or a real error.
        die $_ unless /^ns missing/;
    };

    return $result ? $result->{n} : 0;
}

my $PRIMARY = MongoDB::ReadPreference->new;
my $SEC_PREFERRED = MongoDB::ReadPreference->new( mode => 'secondaryPreferred' );

sub slave_okay {
    my ($self, $value) = @_;
    MongoDB::UsageError->throw("cannot set slave_ok after querying")
      if $self->started_iterating;

    if ($value) {
        # if not 'primary', then slave_ok is already true, so leave alone
        if ( $self->query->read_preference->mode eq 'primary' ) {
            # secondaryPreferred is how mongos interpretes slave_ok
            $self->query->read_preference( $SEC_PREFERRED );
        }
    }
    else {
        $self->query->read_preference( $PRIMARY );
    }

    # returning self is an API change but more consistent with other cursor methods
    return $self;
}


1;

=head1 SYNOPSIS

    while (my $object = $cursor->next) {
        ...
    }

    my @objects = $cursor->all;

=head1 USAGE

=head2 Multithreading

Cursors are cloned in threads, but not reset.  Iterating the same cursor from
multiple threads will give unpredictable results.  Only iterate from a single
thread.

=head1 SEE ALSO

Core documentation on cursors: L<http://dochub.mongodb.org/core/cursors>.

=cut


# vim: ts=4 sts=4 sw=4 et tw=75:
