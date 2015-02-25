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

package MongoDB::Collection;


# ABSTRACT: A MongoDB Collection

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use MongoDB::Error;
use MongoDB::InsertManyResult;
use MongoDB::WriteConcern;
use MongoDB::_Query;
use MongoDB::Op::_BatchInsert;
use MongoDB::Op::_CreateIndexes;
use MongoDB::Op::_Delete;
use MongoDB::Op::_InsertOne;
use MongoDB::Op::_ListIndexes;
use MongoDB::Op::_Update;
use MongoDB::_Types -types;
use Types::Standard -types;
use Tie::IxHash;
use Carp 'carp';
use boolean;
use Safe::Isa;
use Scalar::Util qw/blessed reftype/;
use Syntax::Keyword::Junction qw/any/;
use Try::Tiny;
use Moose;
use namespace::clean -except => 'meta';

has _database => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::Database'],
    required => 1,
);

has _client => (
    is      => 'ro',
    isa     => InstanceOf['MongoDB::MongoClient'],
    lazy    => 1,
    builder => '_build__client',
);

=attr name

The name of the collection.

=cut

has name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

=attr full_name

The full_name of the collection, including the namespace of the database it's
in.

=cut

has full_name => (
    is      => 'ro',
    isa     => Str,
    lazy    => 1,
    builder => '_build_full_name',
);

=attr read_preference

A L<MongoDB::ReadPreference> object.  It may be initialized with a string
corresponding to one of the valid read preference modes or a hash reference
that will be coerced into a new MongoDB::ReadPreference object.

=cut

has read_preference => (
    is       => 'ro',
    isa      => ReadPreference,
    required => 1,
    coerce   => 1,
);

=attr write_concern

A L<MongoDB::WriteConcern> object.  It may be initialized with a hash
reference that will be coerced into a new MongoDB::WriteConcern object.

=cut

has write_concern => (
    is       => 'ro',
    isa      => WriteConcern,
    required => 1,
    coerce   => 1,
);

sub _build__client {
    my ($self) = @_;
    return $self->_database->_client;
}

sub _build_full_name {
    my ($self) = @_;
    my $name    = $self->name;
    my $db_name = $self->_database->name;
    return "${db_name}.${name}";
}


=method clone

    $coll2 = $coll1->clone( write_concern => { w => 2 } );

Constructs a copy of the original collection, but allows changing
attributes in the copy.

=cut

sub clone {
    my ($self, @args) = @_;
    my $class = ref($self);
    if ( @args == 1 && ref( $args[0] ) eq 'HASH' ) {
        return class->new( %$self, %{$args[0]} );
    }

    return $class->new( %$self, @args );
}

=method get_collection

Collection names can be chained together to simulate subcollections joined by a
dot.  For example, the collection C<foo.bar> can be accessed with either of
these expressions:

    my $collection = $db->get_collection( 'foo' )->get_collection( 'bar' );
    my $collection = $db->get_collection( 'foo.bar' );

=cut

sub get_collection {
    my $self = shift @_;
    my $coll = shift @_;

    return $self->_database->get_collection($self->name.'.'.$coll);
}

# utility function to generate an index name by concatenating key/value pairs
sub __to_index_string {
    my $keys = shift;

    my @name;
    if (ref $keys eq 'ARRAY') {
        @name = @$keys;
    }
    elsif (ref $keys eq 'HASH' ) {
        @name = %$keys
    }
    elsif (ref $keys eq 'Tie::IxHash') {
        my @ks = $keys->Keys;
        my @vs = $keys->Values;

        for (my $i=0; $i<$keys->Length; $i++) {
            push @name, $ks[$i];
            push @name, $vs[$i];
        }
    }
    else {
        confess 'expected Tie::IxHash, hash, or array reference for keys';
    }

    return join("_", @name);
}

=method find, query

    my $cursor = $coll->find( $filter );
    my $cursor = $coll->find( $filter, $options );

    my $cursor = $collection->find({ i => { '$gt' => 42 } }, {limit => 20});

Executes a query with the given C<$filter> and returns a C<MongoDB::Cursor> with the results.
C<$filter> can be a hash reference, L<Tie::IxHash>, or array reference (with an
even number of elements).

The query can be customized using L<MongoDB::Cursor> methods, or with an optional
hash reference of options.

Valid options include:

=for :list
* allowPartialResults - get partial results from a mongos if some shards are
  down (instead of throwing an error).
* batchSize – the number of documents to return per batch.
* comment – attaches a comment to the query. If C<$comment> also exists in the
  modifiers document, the comment field overwrites C<$comment>.
* cursorType – indicates the type of cursor to use. It must be one of three
  enumerated values: C<non_tailable> (the default), C<tailable>, and
  C<tailable_await>.
* limit – the maximum number of documents to return.
* maxTimeMS – the maximum amount of time to allow the query to run. If
  C<$maxTimeMS> also exists in the modifiers document, the maxTimeMS field
  overwrites C<$maxTimeMS>.
* modifiers – a hash reference of meta-operators modifying the output or
  behavior of a query.
* noCursorTimeout – if true, prevents the server from timing out a cursor after
  a period of inactivity
* projection - a hash reference defining fields to return. See L<Limit fields
  to
  return|http://docs.mongodb.org/manual/tutorial/project-fields-from-query-results/>
  in the MongoDB documentation for details.
* skip – the number of documents to skip before returning.
* sort – a L<Tie::IxHash> or array reference of key value pairs defining the
  order in which to return matching documents. If C<$orderby> also exists
   * in the modifiers document, the sort field overwrites C<$orderby>.

See also core documentation on querying:
L<http://docs.mongodb.org/manual/core/read/>.

The C<query> method is a legacy alias for C<find>.

=cut

sub find {
    my ( $self, $filter, $opts ) = @_;

    $opts ||= {};
    $opts->{sort} = delete $opts->{sort_by} if $opts->{sort_by};

    my $query = MongoDB::_Query->new(
        db_name         => $self->_database->name,
        coll_name       => $self->name,
        client          => $self->_client,
        read_preference => $self->read_preference,
        filter          => $filter,
        %$opts,
    );

    return MongoDB::Cursor->new( query => $query );
}

=method find_one($query, $fields?, $options?)

    my $object = $collection->find_one({ name => 'Resi' });
    my $object = $collection->find_one({ name => 'Resi' }, { name => 1, age => 1});
    my $object = $collection->find_one({ name => 'Resi' }, {}, {max_time_ms => 100});

Executes the given C<$query> and returns the first object matching it.
C<$query> can be a hash reference, L<Tie::IxHash>, or array reference (with an
even number of elements).  If C<$fields> is specified, the resulting document
will only include the fields given (and the C<_id> field) which can cut down on
wire traffic. If C<$options> is specified, the cursor will be set with the contained options.

=cut

sub find_one {
    my ($self, $query, $fields, $options) = @_;
    $query ||= {};
    $fields ||= {};
    $options ||= {};

    my $cursor = $self->find($query)->limit(-1)->fields($fields);

    for my $key (keys %$options) {

        if (!MongoDB::Cursor->can($key)) {
            confess("$key is not a known method in MongoDB::Cursor");
        }
        $cursor->$key($options->{$key});
    }

    return $cursor->next;
}

=method insert_one

    $res = $coll->insert( $document );

Inserts a single document into the database and returns a
L<MongoDB::InsertOneResult> object.  The document may be a hash reference, an
array reference or a L<Tie::IxHash> object.  If no C<_id> field is present, one
will be added to the original document.

=cut

sub insert_one {
    my ( $self, $document ) = @_;

    $self->_add_oids( [$document] );

    my $op = MongoDB::Op::_InsertOne->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        document      => $document,
        write_concern => $self->write_concern,
    );

    return $self->_client->send_write_op($op);
}

=method insert_many

    $res = $coll->insert_many( [ @documents ] );
    $res = $coll->insert_many( [ @documents ], { ordered => 0 } );

Inserts each of the documents in an array reference into the database and
returns a L<MongoDB::InsertManyResult>.  This is syntactic sugar for
using a Bulk operation.

The documents to be inserted may be hash references, array references or
L<Tie::IxHash> objects.  If no C<_id> field is present in a document, one will
be added to the original document.

An optional hash reference of options may be provided.  The only valid value
is C<ordered>. It defaults to true.  When true, the server will halt after
the first error (if any).  When false, all documents will be processed and
any errors will only be thrown after all insertions are attempted.

On MongoDB servers before version 2.6, C<insert_many> bulk operations are
emulated with individual inserts to capture error information.  On 2.6 or
later, this method will be significantly faster than individual C<insert_one>
calls.

=cut

sub insert_many {
    my ($self, $documents, $opts) = @_;

    confess 'not an array reference' unless ref $documents eq 'ARRAY';
    confess 'not a hash reference' if defined($opts) && ref($opts) ne 'HASH';

    # ordered defaults to true
    my $ordered = exists($opts->{ordered}) ? $opts->{ordered} : 1;

    my $wc = $self->write_concern;
    my $bulk = $ordered ? $self->ordered_bulk : $self->unordered_bulk;
    $bulk->insert($_) for @$documents;
    my $res = $bulk->execute( $wc );
    return MongoDB::InsertManyResult->new(
        acknowledged => $wc->is_safe,
        inserted     => $res->inserted,
    );
}

=method delete_one

    $res = $coll->delete_one( $filter );

Deletes a single document that matches the filter and returns a
L<MongoDB::DeleteResult> object.

The filter provides the L<query
criteria|http://docs.mongodb.org/manual/tutorial/query-documents/> to select a
document for deletion.  It must be a hash reference, array reference or
L<Tie::IxHash> object.

=cut

sub delete_one {
    my ($self, $filter) = @_;

    my $op = MongoDB::Op::_Delete->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $filter,
        just_one      => 1,
        write_concern => $self->write_concern,
    );

    return $self->_client->send_write_op( $op );

}

=method delete_many

    $res = $coll->delete_one( $filter );

Deletes all documents that match the filter and returns a
L<MongoDB::DeleteResult> object.

The filter provides the L<query
criteria|http://docs.mongodb.org/manual/tutorial/query-documents/> to select a
document for deletion.  It must be a hash reference, array reference or
L<Tie::IxHash> object.

=cut

sub delete_many {
    my ($self, $filter) = @_;

    my $op = MongoDB::Op::_Delete->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $filter,
        just_one      => 0,
        write_concern => $self->write_concern,
    );

    return $self->_client->send_write_op( $op );

}

=method replace_one

    $res = $coll->replace_one( $filter, $replacement );
    $res = $coll->replace_one( $filter, $replacement, { upsert => 1 } );

Replaces one document that matches a filter and returns a
L<MongoDB::UpdateResult> object.

The filter provides the L<query
criteria|http://docs.mongodb.org/manual/tutorial/query-documents/> to select a
document for replacement.  It must be a hash reference, array reference or
L<Tie::IxHash> object.

The replacement document must be a hash reference, array reference or
L<Tie::IxHash> object. It must not have any field-update operators in it (e.g.
C<$set>).

An hash reference of options may be provided.  The only valid key is
C<upsert>, which defaults to false.  If provided and true, the replacement
document will be upserted if no matching document exists.

=cut

sub replace_one {
    my ($self, $filter, $replacement, $options) = @_;

    my $op = MongoDB::Op::_Update->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $filter,
        update        => $replacement,
        multi         => false,
        upsert        => $options->{upsert} ? true : false,
        write_concern => $self->write_concern,
    );

    return $self->_client->send_write_op( $op );
}

=method update_one

    $res = $coll->update_one( $filter, $update );
    $res = $coll->update_one( $filter, $update, { upsert => 1 } );

Updates one document that matches a filter and returns a
L<MongoDB::UpdateResult> object.

The filter provides the L<query
criteria|http://docs.mongodb.org/manual/tutorial/query-documents/> to select a
document for update.  It must be a hash reference, array reference or
L<Tie::IxHash> object.

The update document must be a hash reference, array reference or
L<Tie::IxHash> object. It must have only field-update operators in it (e.g.
C<$set>).

An hash reference of options may be provided.  The only valid key is
C<upsert>, which defaults to false.  If provided and true, a new document will
be inserted by taking the filter document and applying the update document
operations to it prior to insertion.

=cut

sub update_one {
    my ($self, $filter, $replacement, $options) = @_;

    my $op = MongoDB::Op::_Update->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $filter,
        update        => $replacement,
        multi         => false,
        upsert        => $options->{upsert} ? true : false,
        write_concern => $self->write_concern,
    );

    return $self->_client->send_write_op( $op );
}

=method update_many

    $res = $coll->update_many( $filter, $update );
    $res = $coll->update_many( $filter, $update, { upsert => 1 } );

Updates multiple document that match a filter and returns a
L<MongoDB::UpdateResult> object.

The filter provides the L<query
criteria|http://docs.mongodb.org/manual/tutorial/query-documents/> to select
documents for update.  It must be a hash reference, array reference or
L<Tie::IxHash> object.

The update document must be a hash reference, array reference or
L<Tie::IxHash> object. It must have only field-update operators in it (e.g.
C<$set>).

An hash reference of options may be provided.  The only valid key is
C<upsert>, which defaults to false.  If provided and true, a new document will
be inserted by taking the filter document and applying the update document
operations to it prior to insertion.

=cut

sub update_many {
    my ($self, $filter, $replacement, $options) = @_;

    my $op = MongoDB::Op::_Update->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $filter,
        update        => $replacement,
        multi         => true,
        upsert        => $options->{upsert} ? true : false,
        write_concern => $self->write_concern,
    );

    return $self->_client->send_write_op( $op );
}


=method update (\%criteria, \%object, \%options?)

    $collection->update({'x' => 3}, {'$inc' => {'count' => -1} }, {"upsert" => 1, "multiple" => 1});

Updates an existing C<$object> matching C<$criteria> in the database.

Returns 1 unless the C<safe> option is set. If C<safe> is set, this will return
a hash of information about the update, including number of documents updated
(C<n>).  If C<safe> is set and the update fails, C<update> will croak. You can
also check if the update succeeded by doing an unsafe update, then calling
L<MongoDB::Database/"last_error($options?)">.

C<update> can take a hash reference of options.  The options currently supported
are:

=over

=item C<upsert>
If no object matching C<$criteria> is found, C<$object> will be inserted.

=item C<multiple|multi>
All of the documents that match C<$criteria> will be updated, not just
the first document found. (Only available with database version 1.1.3 and
newer.)  An error will be throw if both C<multiple> and C<multi> exist
and their boolean values differ.

=item C<safe>
If the update fails and safe is set, the update will croak.

=back

See also core documentation on update: L<http://docs.mongodb.org/manual/core/update/>.

=cut

sub update {
    my ( $self, $query, $object, $opts ) = @_;

    if ( exists $opts->{multiple} ) {
        if ( exists( $opts->{multi} ) && !!$opts->{multi} ne !!$opts->{multiple} ) {
            MongoDB::Error->throw(
                "can't use conflicting values of 'multiple' and 'multi' in 'update'");
        }
        $opts->{multi} = delete $opts->{multiple};
    }

    my $op = MongoDB::Op::_Update->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $query,
        update        => $object,
        multi         => $opts->{multi},
        upsert        => $opts->{upsert},
        write_concern => $self->_dynamic_write_concern($opts),
    );

    my $result = $self->_client->send_write_op( $op );

    # emulate key fields of legacy GLE result
    return {
        ok => 1,
        n => $result->matched_count,
        ( $result->upserted_id ? ( upserted => $result->upserted_id ) : () ),
    };
}

=method find_and_modify

    my $result = $collection->find_and_modify( { query => { ... }, update => { ... } } );

Perform an atomic update. C<find_and_modify> guarantees that nothing else will come along
and change the queried documents before the update is performed.

Returns the old version of the document, unless C<new => 1> is specified. If no documents
match the query, it returns nothing.

=cut

sub find_and_modify {
    my ( $self, $opts ) = @_;

    my $conn = $self->_client;
    my $db   = $self->_database;

    my $result;
    try {
        $result = $db->run_command( [ findAndModify => $self->name, %$opts ] )
    }
    catch {
        die $_ unless $_ eq 'No matching object found';
    };

    return $result->{value} if $result;
    return;
}


=method aggregate

    my $result = $collection->aggregate( [ ... ] );

Run a query using the MongoDB 2.2+ aggregation framework. The first argument is an array-ref of
aggregation pipeline operators.

The type of return value from C<aggregate> depends on how you use it.

=over 4

=item * By default, the aggregation framework returns a document with an embedded array of results, and
the C<aggregate> method returns a reference to that array.

=item * MongoDB 2.6+ supports returning cursors from aggregation queries, allowing you to bypass
the 16MB size limit of documents. If you specifiy a C<cursor> option, the C<aggregate> method
will return a L<MongoDB::QueryResult> object which can be iterated in the normal fashion.

    my $cursor = $collection->aggregate( [ ... ], { cursor => 1 } );

Specifying a C<cursor> option will cause an error on versions of MongoDB below 2.6.

The C<cursor> option may also have some useful options of its own. Currently, the only one
is C<batchSize>, which allows you to control how frequently the cursor must go back to the
database for more documents.

    my $cursor = $collection->aggregate( [ ... ], { cursor => { batchSize => 10 } } );

=item * MongoDB 2.6+ supports an C<explain> option to aggregation queries to retrieve data
about how the server will process a query pipeline.

    my $result = $collection->aggregate( [ ... ], { explain => 1 } );

In this case, C<aggregate> will return a document (not an array) containing the explanation
structure.

=item * Finally, MongoDB 2.6+ will return an empty results array if the C<$out> pipeline operator is used to
write aggregation results directly to a collection. Create a new C<Collection> object to
query the result collection.

=back

See L<Aggregation|http://docs.mongodb.org/manual/aggregation/> in the MongoDB manual
for more information on how to construct aggregation queries.

=cut

sub aggregate {
    my ( $self, $pipeline, $opts ) = @_;
    $opts = ref $opts eq 'HASH' ? $opts : { };

    my $db   = $self->_database;

    if ( exists $opts->{cursor} ) {
        $opts->{cursor} = { } unless ref $opts->{cursor} eq 'HASH';
    }

    # explain requires a boolean
    if ( exists $opts->{explain} ) {
        $opts->{explain} = $opts->{explain} ? true : false;
    }

    my @command = ( aggregate => $self->name, pipeline => $pipeline, %$opts );
    my ($last_op) = keys %{$pipeline->[-1]};
    my $read_pref = $last_op eq '$out' ? undef : $self->read_preference;

    my $op = MongoDB::Op::_Command->new(
        db_name => $db->name,
        query => \@command,
        ( $read_pref ? ( read_preference => $read_pref ) : () )
    );

    my $result = $self->_client->send_read_op( $op );
    my $response = $result->result;

    # if we got a cursor option then we need to construct a wonky cursor
    # object on our end and populate it with the first batch, since
    # commands can't actually return cursors.
    if ( exists $opts->{cursor} ) {
        unless ( exists $response->{cursor} ) {
            die "no cursor returned from aggregation";
        }

        my $qr = MongoDB::QueryResult->new(
            _client => $self->_client,
            address => $result->address,
            cursor  => $response->{cursor},
        );

        return $qr;
    }

    # return the whole response document if they want an explain
    if ( $opts->{explain} ) {
        return $response;
    }

    # TODO: handle errors?

    return $response->{result};
}

=method parallel_scan($max_cursors)

    my @query_results = $collection->parallel_scan(10);

Scan the collection in parallel. The argument is the maximum number of
L<MongoDB::QueryResult> objects to return and must be a positive integer between 1
and 10,000.

As long as the collection is not modified during scanning, each document will
appear only once in one of the cursors' result sets.

Only iteration methods may be called on parallel scan cursors.

If an error occurs, an exception will be thrown.

=cut

sub parallel_scan {
    my ( $self, $num_cursors, $opts ) = @_;
    unless (defined $num_cursors && $num_cursors == int($num_cursors)
        && $num_cursors > 0 && $num_cursors <= 10000
    ) {
        Carp::croak( "first argument to parallel_scan must be a positive integer between 1 and 10000" )
    }
    $opts = ref $opts eq 'HASH' ? $opts : { };

    my $db   = $self->_database;

    my @command = ( parallelCollectionScan => $self->name, numCursors => $num_cursors );

    my $op = MongoDB::Op::_Command->new(
        db_name         => $db->name,
        query           => \@command,
        read_preference => $self->read_preference,
    );

    my $result = $self->_client->send_read_op( $op );
    my $response = $result->result;

    Carp::croak("No cursors returned")
        unless $response->{cursors} && ref $response->{cursors} eq 'ARRAY';

    my @cursors;
    for my $c ( map { $_->{cursor} } @{$response->{cursors}} ) {
        my $qr = MongoDB::QueryResult->new(
            _client => $self->_client,
            address => $result->address,
            cursor  => $c,
        );
        push @cursors, $qr;
    }

    return @cursors;
}

=method rename ("newcollectionname")

    my $newcollection = $collection->rename("mynewcollection");

Renames the collection.  It expects that the new name is currently not in use.

Returns the new collection.  If a collection already exists with that new collection name this will
die.

=cut

sub rename {
    my ($self, $collectionname) = @_;

    my $conn = $self->_client;
    my $database = $conn->get_database( 'admin' );
    my $fullname = $self->full_name;

    my ($db, @collection_bits) = split(/\./, $fullname);
    my $collection = join('.', @collection_bits);
    my $obj = $database->run_command([ 'renameCollection' => "$db.$collection", 'to' => "$db.$collectionname" ]);

    return $conn->get_database( $db )->get_collection( $collectionname );
}


=method ensure_index

    $collection->ensure_index( $keys );
    $collection->ensure_index( $keys, $options );
    $collection->ensure_index(["foo" => 1, "bar" => -1], { unique => 1 });

Makes sure the given C<$keys> of this collection are indexed. C<$keys> can be
an array reference, hash reference, or C<Tie::IxHash>.  Array references or
C<Tie::IxHash> is preferred for multi-key indexes, so that the keys are in the
correct order.  1 creates an ascending index, -1 creates a descending index.

If an optional C<$options> argument is provided, those options are passed
through to the database to modify index creation.  Typical options include:

=for :list
* background – build the index in the background
* name – a name for the index; one will be generated if not provided
* unique – if true, inserting duplicates will fail

See the MongoDB L<index documentation|http://docs.mongodb.org/manual/indexes/>
for more information on indexing and index options.

Returns true on success and throws an exception on failure.

Note: index creation can take longer than the network timeout, resulting
in an exception.  If this is a concern, consider setting the C<background>
option.

=cut

sub ensure_index {
    my ( $self, $keys, $opts ) = @_;
    MongoDB::Error->throw("ensure_index options must be a hash reference")
      if $opts && !ref($opts) eq 'HASH';

    $keys = Tie::IxHash->new(@$keys) if ref $keys eq 'ARRAY';
    $opts = $self->_clean_index_options( $opts, $keys );

    # always use safe write concern for index creation
    my $wc =
        $self->write_concern->is_safe
      ? $self->write_concern
      : MongoDB::WriteConcern->new;

    my $op = MongoDB::Op::_CreateIndexes->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        indexes       => [ { key => $keys, %$opts } ],
        write_concern => $wc,
    );

    $self->_client->send_write_op($op);

    return 1;
}

=method save($doc, $options)

    $collection->save({"author" => "joe"});
    my $post = $collection->find_one;

    $post->{author} = {"name" => "joe", "id" => 123, "phone" => "555-5555"};

    $collection->save( $post );
    $collection->save( $post, { safe => 1 } )

Inserts a document into the database if it does not have an _id field, upserts
it if it does have an _id field.

The return types for this function are a bit of a mess, as it will return the
_id if a new document was inserted, 1 if an upsert occurred, and croak if the
safe option was set and an error occurred.  You can also check if the save
succeeded by doing an unsafe save, then calling
L<MongoDB::Database/"last_error($options?)">.

=cut

sub save {
    my ($self, $doc, $options) = @_;

    if (exists $doc->{"_id"}) {

        if (!$options || !ref $options eq 'HASH') {
            $options = {"upsert" => boolean::true};
        }
        else {
            $options->{'upsert'} = boolean::true;
        }

        return $self->update({"_id" => $doc->{"_id"}}, $doc, $options);
    }
    else {
        return $self->insert($doc, $options);
    }
}


=method count($query?)

    my $n_objects = $collection->count({ name => 'Bob' });

Counts the number of objects in this collection that match the given C<$query>.
If no query is given, the total number of objects in the collection is returned.

=cut

sub count {
    my ($self, $query, $options) = @_;
    $query ||= {};
    $options ||= {};

    my $cursor = $self->find($query);

    for my $key (keys %$options) {

        if (!MongoDB::Cursor->can($key)) {
            confess("$key is not a known method in MongoDB::Cursor");
        }
        $cursor->$key($options->{$key});
    }

    return $cursor->count;
}


=method validate

    $collection->validate;

Asks the server to validate this collection.
Returns a hash of the form:

    {
        'ok' => '1',
        'ns' => 'foo.bar',
        'result' => info
    }

where C<info> is a string of information
about the collection.

=cut

sub validate {
    my ($self, $scan_data) = @_;
    $scan_data = 0 unless defined $scan_data;
    my $obj = $self->_database->run_command({ validate => $self->name });
}


=method drop_indexes

    $collection->drop_indexes;

Removes all indexes from this collection.

=cut

sub drop_indexes {
    my ($self) = @_;
    return $self->drop_index('*');
}

=method drop_index ($index_name)

    $collection->drop_index('foo_1');

Removes an index called C<$index_name> from this collection.
Use C<MongoDB::Collection::get_indexes> to find the index name.

=cut

sub drop_index {
    my ($self, $index_name) = @_;
    return $self->_database->run_command([
        dropIndexes => $self->name,
        index => $index_name,
    ]);
}

=method get_indexes

    my @indexes = $collection->get_indexes;

Returns a list of all indexes of this collection.
Each index contains C<ns>, C<name>, and C<key>
fields of the form:

    {
        'ns' => 'db_name.collection_name',
        'name' => 'index_name',
        'key' => {
            'key1' => dir1,
            'key2' => dir2,
            ...
            'keyN' => dirN
        }
    }

where C<dirX> is 1 or -1, depending on if the
index is ascending or descending on that key.

=cut

sub get_indexes {
    my ($self) = @_;

    my $op = MongoDB::Op::_ListIndexes->new(
        db_name    => $self->_database->name,
        coll_name  => $self->name,
        client     => $self->_client,
        bson_codec => $self->_client,
    );

    my $res = $self->_client->send_read_op($op);

    return $res->all;
}

=method drop

    $collection->drop;

Deletes a collection as well as all of its indexes.

=cut

sub drop {
    my ($self) = @_;
    try {
        $self->_database->run_command({ drop => $self->name });
    }
    catch {
        die $_ unless /ns not found/;
    };
    return;
}

=method initialize_ordered_bulk_op, ordered_bulk

    my $bulk = $collection->initialize_ordered_bulk_op;
    $bulk->insert( $doc1 );
    $bulk->insert( $doc2 );
    ...
    my $result = $bulk->execute;

Returns a L<MongoDB::BulkWrite> object to group write operations into fewer network
round-trips.  This method creates an B<ordered> operation, where operations halt after
the first error. See L<MongoDB::BulkWrite> for more details.

The method C<ordered_bulk> may be used as an alias for C<initialize_ordered_bulk_op>.

=cut

sub initialize_ordered_bulk_op {
    my ($self) = @_;
    return MongoDB::BulkWrite->new( collection => $self, ordered => 1 );
}

=method initialize_unordered_bulk_op, unordered_bulk

This method works just like L</initialize_ordered_bulk_op> except that the order that
operations are sent to the database is not guaranteed and errors do not halt processing.
See L<MongoDB::BulkWrite> for more details.

The method C<unordered_bulk> may be used as an alias for C<initialize_unordered_bulk_op>.

=cut

sub initialize_unordered_bulk_op {
    my ($self) = @_;
    return MongoDB::BulkWrite->new( collection => $self, ordered => 0 );
}

BEGIN {
    # aliases
    no warnings 'once';
    *query = \&find;
    *ordered_bulk = \&initialize_ordered_bulk_op;
    *unordered_bulk = \&initialize_unordered_bulk_op;
}

#--------------------------------------------------------------------------#
# private methods
#--------------------------------------------------------------------------#

sub _add_oids {
    my ($self, $docs) = @_;
    my @ids;

    for my $d ( @$docs ) {
        my $type = reftype($d);
        my $found_id;
        if (ref($d) eq 'Tie::IxHash') {
            $found_id = $d->FETCH('_id');
            unless ( defined $found_id ) {
                $d->Unshift( '_id', $found_id = MongoDB::OID->new );
            }
        }
        elsif ($type eq 'ARRAY') {
            # search for an _id or prepend one
            for my $i ( 0 .. (@$d/2 - 1) ) {
                if ( $d->[2*$i] eq '_id' ) {
                    $found_id = $d->[2*$i+1];
                    last;
                }
            }
            unless (defined $found_id) {
                unshift @$d, '_id', $found_id = MongoDB::OID->new;
            }
        }
        elsif ($type eq 'HASH') {
            # hash or IxHash
            $found_id = $d->{_id};
            unless ( defined $found_id ) {
                $found_id = MongoDB::OID->new;
                $d->{_id} = $found_id;
            }
        }
        else {
            $type = 'scalar' unless $type;
            Carp::croak("unhandled type $type")
        }
        push @ids, $found_id;
    }

    return \@ids;
}

sub _dynamic_write_concern {
    my ( $self, $opts ) = @_;
    if ( !exists( $opts->{safe} ) || $opts->{safe} ) {
        return $self->write_concern;
    }
    else {
        return MongoDB::WriteConcern->new( w => 0 );
    }
}

sub _legacy_index_insert {
    my ($self, $doc, $options) = @_;

    my $wc = $self->_dynamic_write_concern( $options );
    my $result = $self->_client->send_insert($self->full_name, $doc, $wc, undef, 0);

    $result->assert;

    return 1;
}

# old API allowed some snake_case options; some options must
# be turned into booleans
sub _clean_index_options {
    my ( $self, $orig, $keys ) = @_;

    # copy the original so we don't modify it
    my $opts = { $orig ? %$orig : () };

    # add name if not provided
    $opts->{name} = __to_index_string($keys)
      unless defined $opts->{name};

    # safe is no more
    delete $opts->{safe} if exists $opts->{safe};

    # convert snake case
    if ( exists $opts->{drop_dups} ) {
        $opts->{dropDups} = delete $opts->{drop_dups};
    }

    # convert snake case and turn into an integer
    if ( exists $opts->{expire_after_seconds} ) {
        $opts->{expireAfterSeconds} = int( delete $opts->{expire_after_seconds} );
    }

    # convert some things to booleans
    for my $k (qw/unique background sparse dropDups/) {
        next unless exists $opts->{$k};
        $opts->{$k} = boolean( $opts->{$k} );
    }

    return $opts;
}

#--------------------------------------------------------------------------#
# Deprecated legacy methods
#--------------------------------------------------------------------------#

sub insert {
    my ( $self, $document, $opts ) = @_;

    unless ( $opts->{'no_ids'} ) {
        $self->_add_oids( [$document] );
    }

    my $op = MongoDB::Op::_InsertOne->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        document      => $document,
        write_concern => $self->_dynamic_write_concern($opts),
    );

    my $result = $self->_client->send_write_op($op);

    return $result->inserted_id;
}

sub batch_insert {
    my ( $self, $documents, $opts ) = @_;

    confess 'not an array reference' unless ref $documents eq 'ARRAY';

    unless ( $opts->{'no_ids'} ) {
        $self->_add_oids($documents);
    }

    my $op = MongoDB::Op::_BatchInsert->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        documents     => $documents,
        write_concern => $self->_dynamic_write_concern($opts),
    );

    my $result = $self->_client->send_write_op($op);

    my @ids;
    my $inserted_ids = $result->inserted_ids;
    for my $k ( sort { $a <=> $b } keys %$inserted_ids ) {
        push @ids, $inserted_ids->{$k};
    }

    return @ids;
}

sub remove {
    my ($self, $query, $opts) = @_;
    confess "optional argument to remove must be a hash reference"
        if defined $opts && ref $opts ne 'HASH';

    my $op = MongoDB::Op::_Delete->new(
        db_name       => $self->_database->name,
        coll_name     => $self->name,
        filter        => $query,
        just_one      => !! $opts->{just_one},
        write_concern => $self->_dynamic_write_concern($opts),
    );

    my $result = $self->_client->send_write_op( $op );

    # emulate key fields of legacy GLE result
    return {
        ok => 1,
        n => $result->deleted_count,
    };
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=pod

=head1 SYNOPSIS

    # get a Collection via the Database object
    my $coll = $db->get_collection("people");

    # insert a document
    $coll->insert( { name => "John Doe", age => 42 } );

    # find a single document
    my $doc = $coll->find_one( { name => "John Doe" } )

    # Get a MongoDB::Cursor for a query
    my $cursor = $coll->find( { age => 42 } );

=head1 DESCRIPTION

This class models a MongoDB collection and provides an API for interacting
with it.

Generally, you never construct one of these directly with C<new>.  Instead, you
call C<get_collection> on a L<MongoDB::Database> object.

=cut

# vim: ts=4 sts=4 sw=4 et:
