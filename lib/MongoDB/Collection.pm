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
our $VERSION = 'v0.704.4.1';

use Tie::IxHash;
use Carp 'carp';
use boolean;
use Scalar::Util qw/blessed reftype/;
use Try::Tiny;
use Moose;
use namespace::clean -except => 'meta';

has _database => (
    is       => 'ro',
    isa      => 'MongoDB::Database',
    required => 1,
);


has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has full_name => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_full_name',
);

sub _build_full_name {
    my ($self) = @_;
    my $name    = $self->name;
    my $db_name = $self->_database->name;
    return "${db_name}.${name}";
}


sub get_collection {
    my $self = shift @_;
    my $coll = shift @_;

    return $self->_database->get_collection($self->name.'.'.$coll);
}

sub to_index_string {
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


sub _select_cursor_client {
    my ($conn, $ns, $query) = @_;

    return $conn if !$conn->_readpref_pinned || !$conn->find_master;
    return $conn->_master if _cmd_primary_only($ns, $query);
    return $conn->_readpref_pinned;
}

sub _cmd_primary_only {
    my ($ns, $query) = @_;

    # these commands allow read preferences
    my %readpref_commands = (
        'group' => 1,
        'aggregate' => 1,
        'mapreduce' => 1,
        'collstats' => 1,
        'dbstats' => 1,
        'count' => 1,
        'distinct' => 1,
        'geonear' => 1,
        'geosearch' => 1,
        'geowalk' => 1,
        'text' => 1
    );

    if ($ns =~ /\$cmd/) {
        foreach ($query->Keys) {
            return 0 if $readpref_commands{lc($_)};
        }
        return 1;
    }
    else {
        return 0;
    }
}


sub find {
    my ($self, $query, $attrs) = @_;
    # old school options - these should be set with MongoDB::Cursor methods
    my ($limit, $skip, $sort_by) = @{ $attrs || {} }{qw/limit skip sort_by/};

    if ( ! $query ) {
        $query = Tie::IxHash->new();
    }
    elsif ( ref $query eq 'ARRAY' ) {
        $query = Tie::IxHash->new( @$query );
    }
    elsif ( ref $query eq 'HASH' ) {
        $query = Tie::IxHash->new( %$query );
    }
    elsif ( (blessed($query) || '') ne 'Tie::IxHash' ) {
        confess "argument to find must be a hashref, arrayref or Tie::IxHash";
    }


    # if the first key is 'query' we must nest under the '$query' operator
    my @keys = $query->Keys;
    if ( @keys && $keys[0] eq 'query' ) {
        $query = Tie::IxHash->new( '$query' => $query );
    }

    $limit   ||= 0;
    $skip    ||= 0;

    my $conn = $self->_database->_client;
    my $ns = $self->full_name;

    my $slave_ok = ($conn->_readpref_mode == MongoDB::MongoClient->PRIMARY) ||
                   _cmd_primary_only($ns, $query)
                   ? 0 : 1;

    my $cursor = MongoDB::Cursor->new(
        _master    => $conn,
        _client    => _select_cursor_client($conn, $ns, $query),
        _ns        => $ns,
        _query     => $query,
        _limit     => $limit,
        _skip      => $skip,
        slave_okay => $slave_ok
    );

    # add readpref info if connected to mongos
    if ($conn->_readpref_pinned && $conn->_is_mongos && !_cmd_primary_only($ns, $query)) {
        my $modeName = MongoDB::MongoClient->_READPREF_MODENAMES->[$conn->_readpref_mode];
        $cursor->_add_readpref({mode => $modeName, tags => $conn->_readpref_tagsets});
    }

    if ($sort_by) {
        $cursor->sort($sort_by);
    }
    return $cursor;
}

sub query {
    my ($self, $query, $attrs) = @_;

    return $self->find($query, $attrs);
}


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

sub insert { 
    my $self = shift;
    my ( $object, $options ) = @_;
    $self->legacy_insert( @_ );
}

sub legacy_insert {
    my ($self, $object, $options) = @_;

    # XXX if legacy insert doesn't croak on error for unsafe inserts, then we
    # must trap the batch_insert and return whatever is appropriate (probably
    # return undef as that's not a valid OID)
    my ($id) = $self->batch_insert( [ $object ], $options);

    return $id;
}

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

sub batch_insert {
    my ($self, $docs, $options) = @_;

    confess 'not an array reference' unless ref $docs eq 'ARRAY';

    my $ids = [];
    unless ($options->{'no_ids'}) {
        $ids = $self->_add_oids($docs);
    }

    my $conn = $self->_database->_client;
    my $ns = $self->full_name;

    my $insert = MongoDB::_Protocol::write_insert($ns, $docs, 1); # checks keys for "."
    if (length($insert) > $conn->max_bson_size) {
        Carp::croak("insert is too large: ".length($insert)." max: ".$conn->max_bson_size);
    }

    if ( ( defined($options) && $options->{safe} ) or $conn->_w_want_safe ) {
        $self->_make_safe($insert);
    }
    else {
        $conn->send($insert);
    }

    return @$ids;
}

sub _legacy_index_insert {
    my ($self, $doc, $options) = @_;

    my $conn = $self->_database->_client;
    my $ns = $self->full_name;

    my $insert = MongoDB::_Protocol::write_insert($ns, [$doc], 0); # does not check keys for "."

    if ( ( defined($options) && $options->{safe} ) or $conn->_w_want_safe ) {
        $self->_make_safe($insert);
    }
    else {
        $conn->send($insert);
    }

    return;
}

sub update { 
    my $self = shift;
    my ( $query, $object, $opts ) = @_;

    return $self->legacy_update( @_ );
}

sub legacy_update {
    my ($self, $query, $object, $opts) = @_;
    #$self->update_cmd( $query, $object, $opts ) if $self->_database->_client->_use_write_cmd;
    
    # there used to be one option: upsert=0/1
    # now there are two, there will probably be
    # more in the future.  So, to support old code,
    # passing "1" will still be supported, but not
    # documented, so we can phase that out eventually.
    #
    # The preferred way of passing options will be a
    # hash of {optname=>value, ...}
    my $flags = 0;
    if ($opts && ref $opts eq 'HASH') {
        $flags |= $opts->{'upsert'} << 0
            if exists $opts->{'upsert'};
        $flags |= $opts->{'multiple'} << 1
            if exists $opts->{'multiple'};
    }
    else {
        $flags = !(!$opts);
    }

    my $conn = $self->_database->_client;
    my $ns = $self->full_name;

    my $update = MongoDB::_Protocol::write_update($ns, $query, $object, $flags);
    if ($opts->{safe} or $conn->_w_want_safe ) {
        return $self->_make_safe($update);
    }

    if ($conn->send($update) == -1) {
        $conn->connect;
        die("can't get db response, not connected");
    }

    return 1;
}


sub find_and_modify { 
    my ( $self, $opts ) = @_;

    my $conn = $self->_database->_client;
    my $db   = $self->_database;

    my $result;
    try {
        $result = $db->_try_run_command( [ findAndModify => $self->name, %$opts ] )
    }
    catch {
        die $_ unless $_ eq 'No matching object found';
    };

    return $result->{value} if $result;
    return;
}


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
    my $result = $db->_try_run_command( \@command );

    # if we got a cursor option then we need to construct a wonky cursor
    # object on our end and populate it with the first batch, since 
    # commands can't actually return cursors. 
    if ( exists $opts->{cursor} ) { 
        unless ( exists $result->{cursor} ) { 
            die "no cursor returned from aggregation";
        }

        my $cursor = MongoDB::Cursor->new( 
            started_iterating      => 1,              # we have the first batch
            _client                => $db->_client,
            _master                => $db->_client,   # fake this because we're already iterating
            _ns                    => $result->{cursor}{ns},
            _docs                  => $result->{cursor}{firstBatch}, 
            _batch_size            => scalar @{ $result->{cursor}{firstBatch} },  # for has_next
            _query                 => Tie::IxHash->new(@command),
            _cursor_id             => MongoDB::Cursor::_pack_cursor_id($result->{cursor}{id}),
        );

        return $cursor;
    }

    # return the whole result document if they want an explain
    if ( $opts->{explain} ) { 
        return $result;
    }

    # TODO: handle errors?

    return $result->{result};
}

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
    my $result = $db->_try_run_command( \@command );

    Carp::croak("No cursors returned")
        unless $result->{cursors} && ref $result->{cursors} eq 'ARRAY';

    my @cursors;
    for my $c ( map { $_->{cursor} } @{$result->{cursors}} ) {
        # fake up a post-query cursor
        my $cursor = MongoDB::Cursor->new(
            started_iterating      => 1,              # we have the first batch
            _client                => $db->_client,
            _master                => $db->_client,   # fake this because we're already iterating
            _ns                    => $c->{ns},
            _query                 => Tie::IxHash->new(@command),
            _is_parallel           => 1,
            _cursor_id             => MongoDB::Cursor::_pack_cursor_id($c->{id}),
        );

        push @cursors, $cursor;
    }

    return @cursors;
}

sub rename {
    my ($self, $collectionname) = @_;

    my $conn = $self->_database->_client;
    my $database = $conn->get_database( 'admin' );
    my $fullname = $self->full_name;
  
    my ($db, @collection_bits) = split(/\./, $fullname);
    my $collection = join('.', @collection_bits);
    my $obj = $database->_try_run_command([ 'renameCollection' => "$db.$collection", 'to' => "$db.$collectionname" ]);

    return $conn->get_database( $db )->get_collection( $collectionname );
}


sub remove { 
    my $self = shift;
    my ( $query, $options ) = @_;
    $self->legacy_remove( @_ );
}

sub legacy_remove {
    my ($self, $query, $options) = @_;
    #$self->delete_cmd( $query, $options ) if $self->_database->_client->_use_write_cmd;

    my $conn = $self->_database->_client;

    my ($just_one, $safe);
    if (defined $options && ref $options eq 'HASH') {
        $just_one = exists $options->{just_one} ? $options->{just_one} : 0;
        $safe = $options->{safe} or $conn->_w_want_safe;
    }
    else {
        $just_one = $options || 0;
    }

    my $ns = $self->full_name;
    $query ||= {};

    my $remove = MongoDB::_Protocol::write_delete($ns, $query, $just_one);
    if ($safe) {
        return $self->_make_safe($remove);
    }

    if ($conn->send($remove) == -1) {
        $conn->connect;
        die("can't get db response, not connected");
    }

    return 1;
}


sub ensure_index {
    my ($self, $keys, $options, $garbage) = @_;
    my $ns = $self->full_name;

    # we need to use the crappy old api if...
    #  - $options isn't a hash, it's a string like "ascending"
    #  - $keys is a one-element array: [foo]
    #  - $keys is an array with more than one element and the second
    #    element isn't a direction (or at least a good one)
    #  - Tie::IxHash has values like "ascending"
    if (($options && ref $options ne 'HASH') ||
        (ref $keys eq 'ARRAY' &&
         ($#$keys == 0 || $#$keys >= 1 && !($keys->[1] =~ /-?1/))) ||
        (ref $keys eq 'Tie::IxHash' && (my $copy = $keys->[2][0]) =~ /(de|a)scending/)) {
        Carp::croak("you're using the old ensure_index format, please upgrade");
    }

    $keys = Tie::IxHash->new(@$keys) if ref $keys eq 'ARRAY';
    my $obj = Tie::IxHash->new("ns" => $ns, "key" => $keys);

    if (exists $options->{name}) {
        $obj->Push("name" => $options->{name});
    }
    else {
        $obj->Push("name" => MongoDB::Collection::to_index_string($keys));
    }

    foreach ("unique", "background", "sparse") {
        if (exists $options->{$_}) {
            $obj->Push("$_" => ($options->{$_} ? boolean::true : boolean::false));
        }
    }
    if (exists $options->{drop_dups}) {
        $obj->Push("dropDups" => ($options->{drop_dups} ? boolean::true : boolean::false));
    }
    $options->{'no_ids'} = 1;

    foreach ("weights", "default_language", "language_override") {
        if (exists $options->{$_}) {
            $obj->Push("$_" => $options->{$_});
        }
    }

    if (exists $options->{expire_after_seconds}) {
        $obj->Push("expireAfterSeconds" => int($options->{expire_after_seconds}));
    }

    my ($db, $coll) = $ns =~ m/^([^\.]+)\.(.*)/;

    # try the new createIndexes command (mongodb 2.6), falling back to the old insert
    # method if createIndexes is not available.
    my $tmp_ns = $obj->DELETE( 'ns' );     # ci command takes ns outside of index spec

    my $res = $self->_database->get_collection( '$cmd' )->find_one( Tie::IxHash->new( createIndexes => $self->name, indexes => [ $obj ] ) );

    return $res if $res->{ok};    

    # if not ok, no code or code 59 or code 13390 mean "command not available",
    # per DRIVERS-103 and DRIVERS-132
    if ( ( not $res->{ok} )  && 
         ( not exists $res->{code} or $res->{code} == 59 or $res->{code} == 13390) ) { 
        $obj->Unshift( ns => $tmp_ns );     # restore ns to spec
        my $indexes = $self->_database->get_collection("system.indexes");
        return $indexes->_legacy_index_insert($obj, $options);
    } else { 
        die "error creating index: " . $res->{errmsg};
    }
} 


sub _make_safe {
    my ($self, $req) = @_;

    my $ok = $self->_make_safe_cursor($req)->next();

    # $ok->{ok} is 1 if err is set
    Carp::croak $ok->{err} if $ok->{err};
    # $ok->{ok} == 0 is still an error
    if (!$ok->{ok}) {
        Carp::croak $ok->{errmsg};
    }

    return $ok;
}

sub _make_safe_cursor {
    my ($self, $req, $write_concern) = @_;
    my $conn = $self->_database->_client;
    my $db = $self->_database->name;
    $write_concern ||= $conn->_write_concern;

    my $last_error = Tie::IxHash->new(getlasterror => 1, %$write_concern);
    my ($query, $info) = MongoDB::_Protocol::write_query($db.'.$cmd', 0, 0, -1, $last_error);

    my $cursor = MongoDB::Cursor->new(
        _master                         => $conn,
        _client                         => $conn,
        _ns                             => $info->{ns},
        _query                          => Tie::IxHash->new(),
    );

    $cursor->_send_and_recv("$req$query", $info->{request_id});

    return $cursor;
}

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


sub count {
    my ($self, $query) = @_;
    $query ||= {};

    my $obj;
    try {
        $obj = $self->_database->_try_run_command([
            count => $self->name,
            query => $query,
        ]);
    }
    catch {
        # if there was an error, check if it was the "ns missing" one that means the
        # collection hasn't been created or a real error.
        die $_ unless /^ns missing/;
    };

    return $obj ? $obj->{n} : 0;
}


sub validate {
    my ($self, $scan_data) = @_;
    $scan_data = 0 unless defined $scan_data;
    my $obj = $self->_database->run_command({ validate => $self->name });
}


sub drop_indexes {
    my ($self) = @_;
    return $self->drop_index('*');
}


sub drop_index {
    my ($self, $index_name) = @_;
    return $self->_database->run_command([
        dropIndexes => $self->name,
        index => $index_name,
    ]);
}


sub get_indexes {
    my ($self) = @_;
    return $self->_database->get_collection('system.indexes')->query({
        ns => $self->full_name,
    })->all;
}

sub drop {
    my ($self) = @_;
    $self->_database->run_command({ drop => $self->name });
    return;
}

sub initialize_unordered_bulk_op {
    my ($self) = @_;
    return MongoDB::BulkWrite->new( collection => $self, ordered => 0 );
}

sub initialize_ordered_bulk_op {
    my ($self) = @_;
    return MongoDB::BulkWrite->new( collection => $self, ordered => 1 );
}

{
    # shorter aliases for bulk op constructors
    no warnings 'once';
    *ordered_bulk = \&initialize_ordered_bulk_op;
    *unordered_bulk = \&initialize_unordered_bulk_op;
}

__PACKAGE__->meta->make_immutable;

1;




__END__

=pod

=head1 SYNOPSIS

An instance of a MongoDB collection.

    # gets the foo collection
    my $collection = $db->get_collection( 'foo' );

Collection names can be chained together to access subcollections.  For
instance, the collection C<foo.bar> can be accessed with either:

    my $collection = $db->get_collection( 'foo' )->get_collection( 'bar' );

or

    my $collection = $db->get_collection( 'foo.bar' );

=attr name

The name of the collection.

=attr full_name

The full_name of the collection, including the namespace of the database it's
in.


=method get_collection ($name)

    my $collection = $database->get_collection('foo');

Returns a L<MongoDB::Collection> for the collection called C<$name> within this
collection.

=method find($query)

    my $cursor = $collection->find({ i => { '$gt' => 42 } });

Executes the given C<$query> and returns a C<MongoDB::Cursor> with the results.
C<$query> can be a hash reference, L<Tie::IxHash>, or array reference (with an
even number of elements).

The set of fields returned can be limited through the use of the
C<MongoDB::Cursor::fields> method on the resulting L<MongoDB::Cursor> object.
Other commonly used cursor methods are C<MongoDB::Cursor::limit>,
C<MongoDB::Cursor::skip>, and C<MongoDB::Cursor::sort>.

See also core documentation on querying:
L<http://docs.mongodb.org/manual/core/read/>.

=method query($query, $attrs?)

Identical to C<MongoDB::Collection::find>, described above.

    my $cursor = $collection->query->limit(10)->skip(10);

    my $cursor = $collection->query({ location => "Vancouver" })->sort({ age => 1 });


Valid query attributes are:

=over 4

=item limit

Limit the number of results.

=item skip

Skip a number of results.

=item sort_by

Order results.

=back

=method find_one($query, $fields?, $options?)

    my $object = $collection->find_one({ name => 'Resi' });
    my $object = $collection->find_one({ name => 'Resi' }, { name => 1, age => 1});
    my $object = $collection->find_one({ name => 'Resi' }, {}, {max_time_ms => 100});

Executes the given C<$query> and returns the first object matching it.
C<$query> can be a hash reference, L<Tie::IxHash>, or array reference (with an
even number of elements).  If C<$fields> is specified, the resulting document
will only include the fields given (and the C<_id> field) which can cut down on
wire traffic. If C<$options> is specified, the cursor will be set with the contained options.

=method insert ($object, $options?)

    my $id1 = $coll->insert({ name => 'mongo', type => 'database' });
    my $id2 = $coll->insert({ name => 'mongo', type => 'database' }, {safe => 1});

Inserts the given C<$object> into the database and returns it's id
value. C<$object> can be a hash reference, a reference to an array with an
even number of elements, or a L<Tie::IxHash>.  The id is the C<_id> value
specified in the data or a L<MongoDB::OID>.

The optional C<$options> parameter can be used to specify if this is a safe
insert.  A safe insert will check with the database if the insert succeeded and
croak if it did not.  You can also check if the insert succeeded by doing an
unsafe insert, then calling L<MongoDB::Database/"last_error($options?)">.

See also core documentation on insert: L<http://docs.mongodb.org/manual/core/create/>.

=method batch_insert (\@array, $options)

    my @ids = $collection->batch_insert([{name => "Joe"}, {name => "Fred"}, {name => "Sam"}]);

Inserts each of the documents in the array into the database and returns an
array of their _id fields.

The optional C<$options> parameter can be used to specify if this is a safe
insert.  A safe insert will check with the database if the insert succeeded and
croak if it did not. You can also check if the inserts succeeded by doing an
unsafe batch insert, then calling L<MongoDB::Database/"last_error($options?)">.


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

=item C<multiple>
All of the documents that match C<$criteria> will be updated, not just
the first document found. (Only available with database version 1.1.3 and
newer.)

=item C<safe>
If the update fails and safe is set, the update will croak.

=back

See also core documentation on update: L<http://docs.mongodb.org/manual/core/update/>.

=method initialize_ordered_bulk_op

    my $bulk = $collection->initialize_ordered_bulk_op;
    $bulk->insert( $doc1 );
    $bulk->insert( $doc2 );
    ...
    my $result = $bulk->execute;

Returns a L<MongoDB::BulkWrite> object to group write operations into fewer network
round-trips.  This method creates an B<ordered> operation, where operations halt after
the first error. See L<MongoDB::BulkWrite> for more details.

The method C<ordered_bulk> may be used as an alias for C<initialize_ordered_bulk_op>.

=method initialize_unordered_bulk_op

This method works just like L</initialize_ordered_bulk_op> except that the order that
operations are sent to the database is not guaranteed and errors do not halt processing.
See L<MongoDB::BulkWrite> for more details.

The method C<unordered_bulk> may be used as an alias for C<initialize_unordered_bulk_op>.

=method find_and_modify

    my $result = $collection->find_and_modify( { query => { ... }, update => { ... } } );

Perform an atomic update. C<find_and_modify> guarantees that nothing else will come along
and change the queried documents before the update is performed. 

Returns the old version of the document, unless C<new => 1> is specified. If no documents
match the query, it returns nothing.

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
will return a L<MongoDB::Cursor> object which can be iterated in the normal fashion.

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

=method parallel_scan($max_cursors)

    my @cursors = $collection->parallel_scan(10);

Scan the collection in parallel. The argument is the maximum number of
L<MongoDB::Cursor> objects to return and must be a positive integer between 1
and 10,000.

As long as the collection is not modified during scanning, each document will
appear only once in one of the cursors' result sets.

Only iteration methods may be called on parallel scan cursors.

If an error occurs, an exception will be thrown.

=method rename ("newcollectionname")

    my $newcollection = $collection->rename("mynewcollection");

Renames the collection.  It expects that the new name is currently not in use.  

Returns the new collection.  If a collection already exists with that new collection name this will
die.

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


=method remove ($query?, $options?)

    $collection->remove({ answer => { '$ne' => 42 } });

Removes all objects matching the given C<$query> from the database. If no
parameters are given, removes all objects from the collection (but does not
delete indexes, as C<MongoDB::Collection::drop> does).

Returns 1 unless the C<safe> option is set.  If C<safe> is set and the remove
succeeds, C<remove> will return a hash of information about the remove,
including how many documents were removed (C<n>).  If the remove fails and
C<safe> is set, C<remove> will croak.  You can also check if the remove
succeeded by doing an unsafe remove, then calling
L<MongoDB::Database/"last_error($options?)">.

C<remove> can take a hash reference of options.  The options currently supported
are

=over

=item C<just_one>
Only one matching document to be removed.

=item C<safe>
If the update fails and safe is set, this function will croak.

=back

See also core documentation on remove: L<http://docs.mongodb.org/manual/core/delete/>.

=method ensure_index ($keys, $options?)

    use boolean;
    $collection->ensure_index({"foo" => 1, "bar" => -1}, { unique => true });

Makes sure the given C<$keys> of this collection are indexed. C<$keys> can be an
array reference, hash reference, or C<Tie::IxHash>.  C<Tie::IxHash> is preferred
for multi-key indexes, so that the keys are in the correct order.  1 creates an
ascending index, -1 creates a descending index.

If the C<safe> option is not set, C<ensure_index> will not return anything
unless there is a socket error (in which case it will croak).  If the C<safe>
option is set and the index creation fails, it will also croak. You can also
check if the indexing succeeded by doing an unsafe index creation, then calling
L<MongoDB::Database/"last_error($options?)">.

See the L<MongoDB::Indexing> pod for more information on indexing.

=method count($query?)

    my $n_objects = $collection->count({ name => 'Bob' });

Counts the number of objects in this collection that match the given C<$query>.
If no query is given, the total number of objects in the collection is returned.

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

=method drop_indexes

    $collection->drop_indexes;

Removes all indexes from this collection.

=method drop_index ($index_name)

    $collection->drop_index('foo_1');

Removes an index called C<$index_name> from this collection.
Use C<MongoDB::Collection::get_indexes> to find the index name.

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

=method drop

    $collection->drop;

Deletes a collection as well as all of its indexes.


=cut

# vim: ts=4 sts=4 sw=4 et:
