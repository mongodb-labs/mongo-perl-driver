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

use strict;
use warnings;

package MongoDB;
# ABSTRACT: A Mongo Driver for Perl

our $VERSION = '0.26';

use XSLoader;
use MongoDB::Connection;

XSLoader::load(__PACKAGE__, $VERSION);

1;

=head1 NAME

MongoDB - Mongo Driver for Perl

=head1 VERSION

version 0.26

=head1 SYNOPSIS

    use MongoDB;

    my $connection = MongoDB::Connection->new(host => 'localhost', port => 27017);
    my $database   = $connection->get_database('foo');
    my $collection = $database->get_collection('bar');
    my $id         = $collection->insert({ some => 'data' });
    my $data       = $collection->find_one({ _id => $id });

=head1 GETTING HELP

If you have any questions, comments, or complaints, you can get through to the 
developers most dependably via the MongoDB user list: 
I<mongodb-user@googlegroups.com>.  You might be able to get someone quicker
through the MongoDB IRC channel, I<irc.freenode.net#mongodb>.

=head1 AUTHORS

  Florian Ragwitz <rafl@debian.org>
  Kristina Chodorow <kristina@mongodb.org>

=head1 COPYRIGHT AND LICENSE

This software is Copyright (c) 2009 by 10Gen.

This is free software, licensed under:

  The Apache License, Version 2.0, January 2004

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

    $conn   Database connection
    $db     Database
    $coll   Collection
    undef   NULL values are represented by undefined values in Perl
    \@arr   Reference to an array passed to methods
    \%attr  Reference to a hash of attribute values passed to methods

Note that Perl will automatically close and clean up database connections if
all references to them are deleted.

=head2 Outline Usage

To use MongoDB, first you need to load the MongoDB module:

    use MongoDB;
    use strict;
    use warnings;

(The C<use strict;> and C<use warnings;> isn't required, but it's strongly 
recommended.)

Then you need to connect to a Mongo database server.  By default, Mongo listens
for connections on port 27017.  Unless otherwise noted, this documentation 
assumes you are running MongoDB locally on the default port.  

Mongo can be started in I<authentication mode>, which requires clients to log in
before manipulating data.  By default, Mongo does not start in this mode, so no 
username or password is required to make a fully functional connection.  If you
would like to learn more about authentication, see the L<authenticate> method.

To connect to the database, create a new MongoDB Connection object:

    $conn = MongoDB::Connection->new("host" => "localhost", "port" => 27017);

As these are the defaults, we can use the equivalent shorthand:

    $conn = MongoDB::Connection->new;

Connecting is expensive, so try not to open superfluous connections.

There is no way to explicitly disconnect from the database.  When C<$conn> goes
out of scope, the connection will automatically be clased and cleaned up.

=head1 FUNCTIONS

These functions should generally not be used.  They are very low level and have 
nice wrappers in L<MongoDB::Collection>.

=head2 write_insert($ns, \@objs)

    my ($insert, $len, $ids) = MongoDB::write_insert("foo.bar", [{foo => 1}, {bar => -1}, {baz => 1}]);

Creates an insert string to be used by L<MongoDB::Connection::send>.  The second
argument is an array of hashes to insert.  To imitate the behavior of 
L<MongoDB::Collection::insert>, pass a single hash, for example:

    my ($insert, $len, $ids) = MongoDB::write_insert("foo.bar", [{foo => 1}]);

Passing multiple hashes imitates the behavior of 
L<MongoDB::Collection::batch_insert>.

This function returns three values: the string, the length of the string, and an
array of the the _id fields that the inserted hashes will contain.

=head2 write_query($ns, $flags, $skip, $limit, \%query, \%fields?)

    my ($query, $len, $info) = MongoDB::write_query('foo.$cmd', 0, 0, -1, {getlasterror => 1});

Creates a database query to be used by L<MongoDB::Connection::send>.  C<$flags>
are query flags to use (see L<MongoDB::Cursor::Flags> for possible values). 
C<$skip> is the number of results to skip, C<$limit> is the number of results to 
return, C<$query> is the query hash, and C<$fields> is the optional fields to 
return.

This returns the query string, the length of the query string, and a hash of 
information about the query that is used by L<MongoDB::Connection::recv> to get
the database response to the query.

