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

use v5.8.0;
use strict;
use warnings;

package MongoDB;
# ABSTRACT: A MongoDB Driver for Perl

use XSLoader;
use MongoDB::Connection;
use MongoDB::MongoClient;
use MongoDB::Database;
use MongoDB::Collection;
use MongoDB::DBRef;
use MongoDB::OID;

XSLoader::load(__PACKAGE__, $MongoDB::VERSION, int rand(2 ** 24));

1;


__END__



=head1 SYNOPSIS

    use MongoDB;

    my $client     = MongoDB::MongoClient->new(host => 'localhost', port => 27017);
    my $database   = $client->get_database( 'foo' );
    my $collection = $database->get_collection( 'bar' );
    my $id         = $collection->insert({ some => 'data' });
    my $data       = $collection->find_one({ _id => $id });

=head1 INTRO TO MONGODB

This is the Perl driver for MongoDB, a document-oriented database.  This section
introduces some of the basic concepts of MongoDB.  There's also a L<MongoDB::Tutorial/"Tutorial">
POD that introduces using the driver.  For more documentation on MongoDB in
general, check out L<http://www.mongodb.org>.

=head1 GETTING HELP

If you have any questions, comments, or complaints, you can get through to the
developers most dependably via the MongoDB user list:
I<mongodb-user@googlegroups.com>.  You might be able to get someone quicker
through the MongoDB IRC channel, I<irc.freenode.net#mongodb>.

=head1 DESCRIPTION

MongoDB is a database access module.

MongoDB (the database) store all strings as UTF-8.  Non-UTF-8 strings will be
forcibly converted to UTF-8.  To convert something from another encoding to
UTF-8, you can use L<Encode>:

    use Encode;

    my $name = decode('cp932', "\x90\xbc\x96\xec\x81\x40\x91\xbe\x98\x59");
    my $id = $coll->insert( { name => $name, } );

    my $object = $coll->find_one( { name => $name } );

Thanks to taronishino for this example.

=head2 Notation and Conventions

The following conventions are used in this document:

    $client Database client object
    $db     Database
    $coll   Collection
    undef   C<null> values are represented by undefined values in Perl
    \@arr   Reference to an array passed to methods
    \%attr  Reference to a hash of attribute values passed to methods

Note that Perl will automatically close and clean up database connections if
all references to them are deleted.

=head2 Outline Usage

To use MongoDB, first you need to load the MongoDB module:

    use strict;
    use warnings;
    use MongoDB;


Then you need to connect to a MongoDB database server.  By default, MongoDB listens
for connections on port 27017.  Unless otherwise noted, this documentation
assumes you are running MongoDB locally on the default port.

MongoDB can be started in I<authentication mode>, which requires clients to log in
before manipulating data.  By default, MongoDB does not start in this mode, so no
username or password is required to make a fully functional connection.  If you
would like to learn more about authentication, see the C<authenticate> method.

To connect to the database, create a new MongoClient object:

    my $client = MongoDB::MongoClient->new("host" => "localhost:27017");

As this is the default, we can use the equivalent shorthand:

    my $client = MongoDB::MongoClient->new;

Connecting is relatively expensive, so try not to open superfluous connections.

There is no way to explicitly disconnect from the database.  However, the
connection will automatically be closed and cleaned up when no references to
the C<MongoDB::MongoClient> object exist, which occurs when C<$client> goes out of
scope (or earlier if you undefine it with C<undef>).

=head2 INTERNALS

=head3 Class Hierarchy

The classes are arranged in a hierarchy: you cannot create a
L<MongoDB::Collection> instance before you create L<MongoDB::Database> instance,
for example.  The full hierarchy is:

    MongoDB::MongoClient -> MongoDB::Database -> MongoDB::Collection

This is because L<MongoDB::Database> has a field that is a
L<MongoDB::MongoClient> and L<MongoDB::Collection> has a L<MongoDB::Database>
field.

When you call a L<MongoDB::Collection> function, it "trickles up" the chain of
classes.  For example, say we're inserting C<$doc> into the collection C<bar> in
the database C<foo>.  The calls made look like:

=over

=item C<< $collection->insert($doc) >>

Calls L<MongoDB::Database>'s implementation of C<insert>, passing along the
collection name ("foo").

=item C<< $db->insert($name, $doc) >>

Calls L<MongoDB::MongoClient>'s implementation of C<insert>, passing along the
fully qualified namespace ("foo.bar").

=item C<< $client->insert($ns, $doc) >>

L<MongoDB::MongoClient> does the actual work and sends a message to the database.

=back

=head1 FUNCTIONS

These functions should generally not be used.  They are very low level and have
nice wrappers in L<MongoDB::Collection>.

=head2 write_insert($ns, \@objs)

    my ($insert, $ids) = MongoDB::write_insert("foo.bar", [{foo => 1}, {bar => -1}, {baz => 1}]);

Creates an insert string to be used by C<MongoDB::MongoClient::send>.  The second
argument is an array of hashes to insert.  To imitate the behavior of
C<MongoDB::Collection::insert>, pass a single hash, for example:

    my ($insert, $ids) = MongoDB::write_insert("foo.bar", [{foo => 1}]);

Passing multiple hashes imitates the behavior of
C<MongoDB::Collection::batch_insert>.

This function returns the string and an array of the the _id fields that the
inserted hashes will contain.

=head2 write_query($ns, $flags, $skip, $limit, $query, $fields?)

    my ($query, $info) = MongoDB::write_query('foo.$cmd', 0, 0, -1, {getlasterror => 1});

Creates a database query to be used by C<MongoDB::MongoClient::send>.  C<$flags>
are query flags to use (see C<MongoDB::Cursor::Flags> for possible values).
C<$skip> is the number of results to skip, C<$limit> is the number of results to
return, C<$query> is the query hash, and C<$fields> is the optional fields to
return.

This returns the query string and a hash of information about the query that is
used by C<MongoDB::MongoClient::recv> to get the database response to the query.

=head2 write_update($ns, $criteria, $obj, $flags)

    my ($update) = MongoDB::write_update("foo.bar", {age => {'$lt' => 20}}, {'$set' => {young => true}}, 0);

Creates an update that can be used with C<MongoDB::MongoClient::send>.  C<$flags>
can be 1 for upsert and/or 2 for updating multiple documents.

=head2 write_remove($ns, $criteria, $flags)

    my ($remove) = MongoDB::write_remove("foo.bar", {name => "joe"}, 0);

Creates a remove that can be used with C<MongoDB::MongoClient::send>.  C<$flags>
can be 1 for removing just one matching document.

=head2 read_documents($buffer)

  my @documents = MongoDB::read_documents($buffer);

Decodes BSON documents from the given buffer.

=head1 SEE ALSO

MongoDB main website L<http://www.mongodb.org/>

Core documentation L<http://www.mongodb.org/display/DOCS/Manual>

L<MongoDB::Tutorial>, L<MongoDB::Examples>
