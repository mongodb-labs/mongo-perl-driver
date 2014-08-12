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

use 5.008;
use strict;
use warnings;

package MongoDB;
# ABSTRACT: Official MongoDB Driver for Perl

use version;
our $VERSION = 'v0.704.4.1';

# regexp_pattern was unavailable before 5.10, had to be exported to load the
# function implementation on 5.10, and was automatically available in 5.10.1
use if ($] eq '5.010000'), 're', 'regexp_pattern';

use Carp ();
use MongoDB::BSON;
use MongoDB::MongoClient;
use MongoDB::Database;
use MongoDB::Collection;
use MongoDB::DBRef;
use MongoDB::OID;
use MongoDB::Timestamp;
use MongoDB::BSON::Binary;
use MongoDB::BSON::Regexp;
use MongoDB::BulkWrite;
use MongoDB::_Link;
use MongoDB::_Protocol;

*read_documents = \&MongoDB::BSON::decode_bson;

# regexp_pattern was unavailable before 5.10, had to be exported to load the
# function implementation on 5.10, and was automatically available in 5.10.1
if ( $] eq '5.010' ) {
    require re;
    re->import('regexp_pattern');
}

sub force_double {
    if ( ref $_[0] ) {
        Carp::croak("Can't force a reference into a double");
    }
    return $_[0] = unpack("d",pack("d", $_[0]));
}

sub force_int {
    if ( ref $_[0] ) {
        Carp::croak("Can't force a reference into an int");
    }
    return $_[0] = int($_[0]);
}

1;


__END__

=head1 SYNOPSIS

    use MongoDB;

    my $client     = MongoDB::MongoClient->new(host => 'localhost', port => 27017);
    my $database   = $client->get_database( 'foo' );
    my $collection = $database->get_collection( 'bar' );
    my $id         = $collection->insert({ some => 'data' });
    my $data       = $collection->find_one({ _id => $id });

=head1 DESCRIPTION

This is the official Perl driver for MongoDB.  MongoDB is an open-source
document database that provides high performance, high availability, and easy
scalability.

A MongoDB server (or cluster) hosts a number of databases. A database holds a
set of collections. A collection holds a set of documents. A document is a set
of key-value pairs. Documents have dynamic schema. Dynamic schema means that
documents in the same collection do not need to have the same set of fields or
structure, and common fields in a collection's documents may hold different
types of data.

Here are some resources for learning more about MongoDB:

=for :list
* L<MongoDB Manual|http://docs.mongodb.org/manual/contents/>
* L<MongoDB CRUD Introduction|http://docs.mongodb.org/manual/core/crud-introduction/>
* L<MongoDB Data Modeling Introductions|http://docs.mongodb.org/manual/core/data-modeling-introduction/>

For getting started with the Perl driver, see these pages:

=for :list
* L<MongoDB Perl Driver Tutorial|MongoDB::Tutorial>
* L<MongoDB Perl Driver Examples|MongoDB::Examples>

Extensive documentation and support resources are available via the
L<MongoDB community website|http://www.mongodb.org/>.

=head1 USAGE

The MongoDB driver is organized into a set of classes representing different
levels of abstraction and functionality.

As a user, you first create and configure a L<MongoDB::MongoClient> object to
connect to a MongoDB server (or cluster).  From that client object, you can get
a L<MongoDB::Database> object for interacting with a specific database.

From a database object you can get a L<MongoDB::Collection> object for CRUD
operations on that specific collection, or a L<MongoDB::GridFS> object for
working with an abstract file system hosted on the database.  Each of those
classes may return other objects for specific features or functions.

See the documentation of those classes for more details or the
L<MongoDB Perl Driver Tutorial|MongoDB::Tutorial> for an example.

=head1 FUNCTIONS (DEPRECATED)

The following low-level functions are deprecated and will be removed in a
future release.

=for :list
* write_insert
* write_query
* write_update
* write_remove
* read_documents

=head1 SEMANTIC VERSIONING SCHEME

Starting with MongoDB v0.704.0.0, the driver will be using a modified
L<semantic versioning|http://semver.org/> scheme.

Versions will have a C<vX.Y.Z.N> tuple scheme with the following properties:

=for :list
* C<X> will be incremented for incompatible API changes
* C<Y> will be incremented for new functionality that is backwards compatible
* C<Z> will be incremented for backwards-compatible bug fixes
* C<N> will be zero for a stable release; C<N> will be non-zero for development releases

We use C<N> because CPAN does not support pre-release version labels (e.g.
"-alpha1") and requires non-decreasing version numbers for releases.

When C<N> is non-zero, C<X>, C<Y>, and C<Z> have no semantic meaning except to
indicate the last stable release.

For example, v0.704.0.1 is merely the first development release after
v0.704.0.0.  The next stable release could be a bug fix (v0.704.1.0), a feature
enhancement (v0.705.0.0), or an API change (v1.0.0.0).

See the Changes file included with development releases for an indication of
the nature of changes involved.


