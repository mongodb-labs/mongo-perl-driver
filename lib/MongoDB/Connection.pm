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

package MongoDB::Connection;
our $VERSION = '0.33';

# ABSTRACT: A connection to a Mongo server

use MongoDB;
use MongoDB::Cursor;

use Any::Moose;
use Any::Moose 'Util::TypeConstraints';
use Digest::MD5;
use Tie::IxHash;
use boolean;

=head1 NAME

MongoDB::Connection - A connection to a Mongo server

=head1 SYNOPSIS

The MongoDB::Connection class creates a connection to the MongoDB server. 

By default, it connects to a single server running on the local machine 
listening on the default port:

    # connects to localhost:27017
    my $connection = MongoDB::Connection->new;

It can connect to a database server running anywhere, though:

    my $connection = MongoDB::Connection->new(host => 'example.com:12345');

See the L</"host"> section for more options for connecting to MongoDB.

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

=cut

has host => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'mongodb://localhost:27017',
);

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

=cut

has w => (
    is      => 'rw',
    isa     => 'Int',
    default => 1,
);

=head2 wtimeout

The number of milliseconds an operation should wait for C<w> slaves to replicate 
it.

Defaults to 1000 (1 second).

See C<w> above for more information.

=cut

has wtimeout => (
    is      => 'rw',
    isa     => 'Int',
    default => 1000,
);

has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017,
);

has left_host => (
    is       => 'ro',
    isa      => 'Str',
);

has left_port => (
    is       => 'ro',
    isa      => 'Int',
    default  => 27017,
);

has right_host => (
    is       => 'ro',
    isa      => 'Str',
);

has right_port => (
    is       => 'ro',
    isa      => 'Int',
    default  => 27017,
);

=head2 auto_reconnect

Boolean indicating whether or not to reconnect if the connection is
interrupted. Defaults to C<1>.

=cut

has auto_reconnect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 1,
);

=head2 auto_connect

Boolean indication whether or not to connect automatically on object
construction. Defaults to C<1>.

=cut

has auto_connect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 1,
);

=head2 timeout

Connection timeout in milliseconds. Defaults to C<20000>.

=cut

has timeout => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 20000,
);

=head2 username

Username for this connection.  Optional.  If this and the password field are 
set, the connection will attempt to authenticate on connection/reconnection.

=cut

has username => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

=head2 password

Password for this connection.  Optional.  If this and the username field are 
set, the connection will attempt to authenticate on connection/reconnection.

=cut

has password => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

=head2 db_name

Database to authenticate on for this connection.  Optional.  If this, the 
username, and the password fields are set, the connection will attempt to 
authenticate against this database on connection/reconnection.  Defaults to
"admin".

=cut

has db_name => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    default  => 'admin',
);

=head2 query_timeout

This will cause all queries (including C<find_one>s and C<run_command>s) to die
after this period if the database has not responded.

This value is in milliseconds and defaults to the value of 
L<MongoDB::Cursor/timeout>.

A value of -1 will cause the driver to wait forever for responses and 0 will 
cause it to die immediately.

This value overrides L<MongoDB::Cursor/timeout>.

=cut

has query_timeout => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => sub { return $MongoDB::Cursor::timeout; },
);


=head2 port [deprecated]

B<Use L</host> instead.>

Port to use when connecting. Defaults to C<27017>.

=head2 left_host [deprecated]

B<Use L</host> instead.>

Paired connection host to connect to. Can be master or slave.

=head2 left_port [deprecated]

B<Use L</host> instead.>

Port to use when connecting to left_host. Defaults to C<27017>.

=head2 right_host [deprecated]

B<Use L</host> instead.>

Paired connection host to connect to. Can be master or slave.

=head2 right_port [deprecated]

B<Use L</host> instead.>

Port to use when connecting to right_host. Defaults to C<27017>.

=cut


sub CLONE_SKIP { 1 }

sub AUTOLOAD {
    my $self = shift @_;
    our $AUTOLOAD;

    my $db = $AUTOLOAD;
    $db =~ s/.*:://;

    return $self->get_database($db);
}

sub _get_hosts {
    my ($self) = @_;
    my @hosts;

    # deprecated syntax
    if (!($self->host =~ /^mongodb:\/\//)) {
        push @hosts, {host => $self->host, port => $self->port};
        return @hosts;
    }
    elsif ($self->left_host && $self->right_host) {
        push @hosts, {host => $self->left_host, port => $self->left_port};
        push @hosts, {host => $self->right_host, port => $self->right_port};
        return @hosts;
    }

    my $str = substr $self->host, 10;

    my @pairs = split ",", $str;

    foreach (@pairs) {
        my @hp = split ":", $_;

        if (!exists $hp[1]) {
            $hp[1] = 27017;
        }

        push @hosts, {host => $hp[0], port => $hp[1]};
    }

    return @hosts;
}

sub BUILD {
    my ($self) = @_;
    eval "use ${_}" # no Any::Moose::load_class becase the namespaces already have symbols from the xs bootstrap
        for qw/MongoDB::Database MongoDB::Cursor MongoDB::OID/;

    my @hosts = $self->_get_hosts;
    $self->_init_conn(\@hosts);

    if ($self->auto_connect) {
        $self->connect;

        if (defined $self->username && defined $self->password) {
            $self->authenticate($self->db_name, $self->username, $self->password);
        }
    }
}

=head1 METHODS

=head2 connect

    $connection->connect;

Connects to the mongo server. Called automatically on object construction if
C<auto_connect> is true.

=head2 database_names

    my @dbs = $connection->database_names;

Lists all databases on the mongo server.

=cut

sub database_names {
    my ($self) = @_;
    my $ret = $self->get_database('admin')->run_command({ listDatabases => 1 });
    return map { $_->{name} } @{ $ret->{databases} };
}

=head2 get_database($name)

    my $database = $connection->get_database('foo');

Returns a L<MongoDB::Database> instance for database with the given C<$name>.

=cut

sub get_database {
    my ($self, $database_name) = @_;
    return MongoDB::Database->new(
        _connection => $self,
        name        => $database_name,
    );
}

=head2 find_master

    $master = $connection->find_master

Determines which host of a paired connection is master.  Does nothing for
a non-paired connection.  This need never be invoked by a user, it is 
called automatically by internal functions.  Returns the index of the master
connection in the list of connections or -1 if it cannot be determined.

=cut

sub find_master {
    my ($self) = @_;
    # return if the connection isn't paired
    if (!(defined $self->left_host) || !(defined $self->right_host)) {
        my @servers = $self->_get_hosts;
        return -1 unless @servers;

        my $index = 0;
        foreach (@servers) {
            my $conn;
            eval {
                $conn = MongoDB::Connection->new("host" => $_->{host}, "port" => $_->{port}, timeout => $self->timeout);
            };
            if (!($@ =~ m/couldn't connect to server/)) {
                my $master = $conn->admin->run_command({ismaster => 1});
                if ($master->{'ismaster'}) {    
                    return $index;
                }
            }

            $index++;
        }

        return -1;
    }

    my ($left, $right, $master);

    # check the left host
    eval {
        $left = MongoDB::Connection->new("host" => $self->left_host, "port" => $self->left_port, timeout => $self->timeout);
    };
    if (!($@ =~ m/couldn't connect to server/)) {
        $master = $left->find_one('admin.$cmd', {ismaster => 1});
        if ($master->{'ismaster'}) {    
            return 0;
        }
    }

    # check the right_host
    eval {
        $right = MongoDB::Connection->new("host" => $self->right_host, "port" => $self->right_port, timeout => $self->timeout);
    };
    if (!($@ =~ m/couldn't connect to server/)) {
        $master = $right->find_one('admin.$cmd', {ismaster => 1});
        if ($master->{'ismaster'}) {
            return 1;
        }
    }

    # something went wrong
    return -1;
}

=head2 authenticate ($dbname, $username, $password, $is_digest?)

    $connection->authenticate('foo', 'username', 'secret');

Attempts to authenticate for use of the C<$dbname> database with C<$username>
and C<$password>. Passwords are expected to be cleartext and will be
automatically hashed before sending over the wire, unless C<$is_digest> is
true, which will assume you already did the hashing on yourself.

See also the core documentation on authentication: 
L<http://dochub.mongodb.org/core/authentication>.

=cut

sub authenticate {
    my ($self, $dbname, $username, $password, $is_digest) = @_;
    my $hash = $password;

    # create a hash if the password isn't yet encrypted
    if (!$is_digest) {
        $hash = Digest::MD5::md5_hex("${username}:mongo:${password}");
    }

    # get the nonce
    my $db = $self->get_database($dbname);
    my $result = $db->run_command({getnonce => 1});
    if (!$result->{'ok'}) {
        return $result;
    }

    my $nonce = $result->{'nonce'};
    my $digest = Digest::MD5::md5_hex($nonce.$username.$hash);

    # run the login command
    my $login = tie(my %hash, 'Tie::IxHash');
    %hash = (authenticate => 1,
             user => $username, 
             nonce => $nonce,
             key => $digest);
    $result = $db->run_command($login);

    return $result;
}

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

=cut

no Any::Moose;
__PACKAGE__->meta->make_immutable (inline_destructor => 0);

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
