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

package MongoDB::Connection;

# ABSTRACT: A connection to a MongoDB server (DEPRECATED)

use Moose;

use MongoDB;
use MongoDB::Cursor;
use MongoDB::MongoClient;

use Class::MOP::Class;
use Digest::MD5;
use Tie::IxHash;
use Carp 'carp';
use boolean;


has '_client' => (
    isa         => 'MongoDB::MongoClient', 
    is          => 'ro',
    handles     => [ grep { $_ !~ /^(meta|new)$/ } 
                     map { $_->name } Class::MOP::Class->initialize( 'MongoDB::MongoClient' )->get_all_methods 
                   ]
);


around 'new' => sub { 
    my ( $orig, $self, @args ) = @_;
    return $self->$orig( _client => MongoDB::MongoClient->new( @args ) );
};



sub AUTOLOAD {
    my $self = shift @_;
    our $AUTOLOAD;

    my $db = $AUTOLOAD;
    $db =~ s/.*:://;

    carp sprintf q{AUTOLOADed database method names are deprecated and will be removed in a future release. Use $client->get_database( '%s' ) instead.}, $db;

    return $self->get_database($db);
}



__PACKAGE__->meta->make_immutable ( inline_destructor => 0, inline_constructor => 0 );

1;





__END__

=pod

=head1 DEPRECATED

NOTE: C<MongoDB::Connection> is DEPRECATED as of version 0.502.0 of the MongoDB CPAN distribution. 
It is no longer maintained and will be removed in a future version. Use L<MongoDB::MongoClient> instead. 

=head1 SYNOPSIS

The MongoDB::Connection class creates a connection to the MongoDB server.

By default, it connects to a single server running on the local machine
listening on the default port:

    # connects to localhost:27017
    my $connection = MongoDB::Connection->new;

It can connect to a database server running anywhere, though:

    my $connection = MongoDB::Connection->new(host => 'example.com:12345');

See the L</"host"> section for more options for connecting to MongoDB.

=head2 MULTITHREADING

Cloning instances of this class is disabled in Perl 5.8.7+, so forked threads
will have to create their own connections to the database.

=head1 SEE ALSO

Core documentation on connections: L<http://dochub.mongodb.org/core/connections>.

=head1 ATTRIBUTES

=head2 host

Server or servers to connect to. Defaults to C<mongodb://localhost:27017>.

To connect to more than one database server, use the format:

    mongodb://host1[:port1][,host2[:port2],...[,hostN[:portN]]]

An arbitrary number of hosts can be specified.

The connect method will return success if it can connect to at least one of the
hosts listed.  If it cannot connect to any hosts, it will die.

If a port is not specified for a given host, it will default to 27017. For
example, to connecting to C<localhost:27017> and C<localhost:27018>:

    $conn = MongoDB::Connection->new("host" => "mongodb://localhost,localhost:27018");

This will succeed if either C<localhost:27017> or C<localhost:27018> are available.

The connect method will also try to determine who is master if more than one
server is given.  It will try the hosts in order from left to right.  As soon as
one of the hosts reports that it is master, the connect will return success.  If
no hosts report themselves as masters, the connect will die, reporting that it
could not find a master.

If username and password are given, success is conditional on being able to log
into the database as well as connect.  By default, the driver will attempt to
authenticate with the admin database.  If a different database is specified
using the C<db_name> property, it will be used instead.

=head2 w

I<Only supported in MongoDB server version 1.5+.>

The default number of mongod slaves to replicate a change to before reporting
success for all operations on this collection.

Defaults to 1 (just the current master).

If this is not set, a safe insert will wait for 1 machine (the master) to
ack the operation, then return that it was successful.  If the master has
slaves, the slaves may not yet have a record of the operation when success is
reported.  Thus, if the master goes down, the slaves will never get this
operation.

To prevent this, you can set C<w> to a value greater than 1.  If you set C<w> to
<N>, it means that safe operations must have succeeded on the master and C<N-1>
slaves before the client is notified that the operation succeeded.  If the
operation did not succeed or could not be replicated to C<N-1> slaves within the
timeout (see C<wtimeout> below), the safe operation will fail (croak).

Some examples of a safe insert with C<w> set to 3 and C<wtimeout> set to 100:

=over 4

=item The master inserts the document, but 100 milliseconds pass before the
slaves have a chance to replicate it.  The master returns failure and the client
croaks.

=item The master inserts the document and two or more slaves replicate the
operation within 100 milliseconds.  The safe insert returns success.

=item The master inserts the document but there is only one slave up.  The
safe insert times out and croaks.

=back

I<MongoDB server version 2.0+: "majority" and Data Center Awareness>

As of MongoDB 2.0+, the 'w' parameter can be passed strings. This can be done by passing it the string "majority" this will wait till the B<majority> of 
of the nodes in the replica set have recieved the data. For more information see: http://www.mongodb.org/display/DOCS/getLastError+Command#getLastErrorCommand-majority

This can be useful for "Data Center Awareness." In v2.0+, you can "tag" replica members. With "tagging" you can specify a new "getLastErrorMode" where you can create new
rules on how your data is replicated. To used you getLastErrorMode, you pass in the name of the mode to the 'w' parameter. For more infomation see: http://www.mongodb.org/display/DOCS/Data+Center+Awareness

=head2 wtimeout

The number of milliseconds an operation should wait for C<w> slaves to replicate
it.

Defaults to 1000 (1 second).

See C<w> above for more information.

=head2 j

If true, awaits the journal commit before returning. If the server is running without journaling, it returns immediately, and successfully.


=head2 auto_reconnect

Boolean indicating whether or not to reconnect if the connection is
interrupted. Defaults to C<1>.

=head2 auto_connect

Boolean indication whether or not to connect automatically on object
construction. Defaults to C<1>.

=head2 timeout

Connection timeout in milliseconds. Defaults to C<20000>.

=head2 username

Username for this connection.  Optional.  If this and the password field are
set, the connection will attempt to authenticate on connection/reconnection.

=head2 password

Password for this connection.  Optional.  If this and the username field are
set, the connection will attempt to authenticate on connection/reconnection.

=head2 db_name

Database to authenticate on for this connection.  Optional.  If this, the
username, and the password fields are set, the connection will attempt to
authenticate against this database on connection/reconnection.  Defaults to
"admin".

=head2 query_timeout

    # set query timeout to 1 second
    my $conn = MongoDB::Connection->new(query_timeout => 1000);

    # set query timeout to 6 seconds
    $conn->query_timeout(6000);

This will cause all queries (including C<find_one>s and C<run_command>s) to die
after this period if the database has not responded.

This value is in milliseconds and defaults to the value of
L<MongoDB::Cursor/timeout>.

    $MongoDB::Cursor::timeout = 5000;
    # query timeout for $conn will be 5 seconds
    my $conn = MongoDB::Connection->new;

A value of -1 will cause the driver to wait forever for responses and 0 will
cause it to die immediately.

This value overrides L<MongoDB::Cursor/timeout>.

    $MongoDB::Cursor::timeout = 1000;
    my $conn = MongoDB::Connection->new(query_timeout => 10);
    # timeout for $conn is 10 milliseconds

=head2 max_bson_size

This is the largest document, in bytes, storable by MongoDB. The driver queries
MongoDB on connection to determine this value.  It defaults to 4MB.

=head2 find_master

If this is true, the driver will attempt to find a master given the list of
hosts.  The master-finding algorithm looks like:

    for host in hosts

        if host is master
             return host

        else if host is a replica set member
            master := replica set's master
            return master

If no master is found, the connection will fail.

If this is not set (or set to the default, 0), the driver will simply use the
first host in the host list for all connections.  This can be useful for
directly connecting to slaves for reads.

If you are connecting to a slave, you should check out the
L<MongoDB::Cursor/slave_okay> documentation for information on reading from a
slave.

You can use the C<ismaster> command to find the members of a replica set:

    my $result = $db->run_command({ismaster => 1});

The primary and secondary hosts are listed in the C<hosts> field, the slaves are
in the C<passives> field, and arbiters are in the C<arbiters> field.

=head2 ssl

This tells the driver that you are connecting to an SSL mongodb instance.

This option will be ignored if the driver was not compiled with the SSL flag. You must
also be using a database server that supports SSL.


=head2 dt_type

Sets the type of object which is returned for DateTime fields. The default is L<DateTime>. Other
acceptable values are L<DateTime::Tiny> and C<undef>. The latter will give you the raw epoch value
rather than an object.


=head1 METHODS

=head2 connect

    $connection->connect;

Connects to the mongo server. Called automatically on object construction if
C<auto_connect> is true.

=head2 database_names

    my @dbs = $connection->database_names;

Lists all databases on the mongo server.

=head2 get_database($name)

    my $database = $connection->get_database('foo');

Returns a L<MongoDB::Database> instance for database with the given C<$name>.


=head2 get_master

    $master = $connection->get_master

Determines which host of a paired connection is master.  Does nothing for
a non-paired connection.  This need never be invoked by a user, it is
called automatically by internal functions.  Returns the index of the master
connection in the list of connections or -1 if it cannot be determined.

=head2 authenticate ($dbname, $username, $password, $is_digest?)

    $connection->authenticate('foo', 'username', 'secret');

Attempts to authenticate for use of the C<$dbname> database with C<$username>
and C<$password>. Passwords are expected to be cleartext and will be
automatically hashed before sending over the wire, unless C<$is_digest> is
true, which will assume you already did the hashing on yourself.

See also the core documentation on authentication:
L<http://dochub.mongodb.org/core/authentication>.


=head2 send($str)

    my ($insert, $ids) = MongoDB::write_insert('foo.bar', [{name => "joe", age => 40}]);
    $conn->send($insert);

Low-level function to send a string directly to the database.  Use
L<MongoDB::write_insert>, L<MongoDB::write_update>, L<MongoDB::write_remove>, or
L<MongoDB::write_query> to create a valid string.

=head2 recv(\%info)

    my $cursor = $conn->recv({ns => "foo.bar"});

Low-level function to receive a response from the database. Returns a
C<MongoDB::Cursor>.  At the moment, the only required field for C<$info> is
"ns", although "request_id" is likely to be required in the future.  The
C<$info> hash will be automatically created for you by L<MongoDB::write_query>.


=head2 fsync(\%args)

    $conn->fsync();

A function that will forces the server to flush all pending writes to the storage layer.

The fsync operation is synchronous by default, to run fsync asynchronously, use the following form:

    $conn->fsync({async => 1});

The primary use of fsync is to lock the database during backup operations. This will flush all data to the data storage layer and block all write operations until you unlock the database. Note: you can still read while the database is locked. 

    $conn->fsync({lock => 1});

=head2 fsync_unlock()

    $conn->fsync_unlock();

Unlocks a database server to allow writes and reverses the operation of a $conn->fsync({lock => 1}); operation. 
