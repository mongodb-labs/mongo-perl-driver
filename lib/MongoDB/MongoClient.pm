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

package MongoDB::MongoClient;

# ABSTRACT: A connection to a MongoDB server

use Moose;
use Moose::Util::TypeConstraints;
use MongoDB;
use MongoDB::Cursor;
use MongoDB::BSON::Binary;
use Digest::MD5;
use Tie::IxHash;
use Carp 'carp', 'croak';
use Scalar::Util 'reftype';
use boolean;
use Encode;

has host => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'mongodb://localhost:27017',
);

has w => (
    is      => 'rw',
    isa     => 'Int|Str',
    default => 1,
);

has wtimeout => (
    is      => 'rw',
    isa     => 'Int',
    default => 1000,
);

has j => (
    is      => 'rw',
    isa     => 'Bool',
    default => 0
);


has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017,
);


has auto_reconnect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 1,
);

has auto_connect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 1,
);

has timeout => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 20000,
);

has username => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has password => (
    is       => 'rw',
    isa      => 'Str',
    required => 0,
);

has db_name => (
    is       => 'rw',
    isa      => 'Str',
    required => 1,
    default  => 'admin',
);

has query_timeout => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => sub { return $MongoDB::Cursor::timeout; },
);

has max_bson_size => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => 4194304
);

has find_master => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0,
);


has ssl => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0,
);

has sasl => ( 
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0
);

has sasl_mechanism => ( 
    is       => 'ro',
    isa      => subtype( Str => where { /^GSSAPI|PLAIN$/ } ),
    required => 1,
    default  => 'GSSAPI',
);

# hash of servers in a set
# call connected() to determine if a connection is enabled
has _servers => (
    is       => 'rw',
    isa      => 'HashRef',
    default => sub { {} },
);

# actual connection to a server in the set
has _master => (
    is       => 'rw',
#    isa      => 'MongoDB::Connection',
    required => 0,
);

has ts => (
    is      => 'rw',
    isa     => 'Int',
    default => 0
);


has dt_type => (
    is      => 'rw',
    required => 0,
    default  => 'DateTime'
);

has inflate_dbrefs => (
    is        => 'rw',
    isa       => 'Bool',
    required  => 0,
    default   => 1
);

sub BUILD {
    my ($self, $opts) = @_;
    eval "use ${_}" # no Any::Moose::load_class becase the namespaces already have symbols from the xs bootstrap
        for qw/MongoDB::Database MongoDB::Cursor MongoDB::OID MongoDB::Timestamp/;

    my @pairs;

    # supported syntax (see http://docs.mongodb.org/manual/reference/connection-string/)
    if ($self->host =~ m{ ^
            mongodb://
            (?: ([^:]*) : ([^@]*) @ )? # [username:password@]
            ([^/]*) # host1[:port1][,host2[:port2],...[,hostN[:portN]]]
            (?:
               / ([^?]*) # /[database]
                (?: [?] (.*) )? # [?options]
            )?
            $ }x ) {
        my ($username, $password, $hostpairs, $database, $options) = ($1, $2, $3, $4, $5);

        # we add these things to $opts as well as self so that they get propagated when we recurse for multiple servers
        $self->username($opts->{username} = $username) if $username;
        $self->password($opts->{password} = $password) if $password;
        $self->db_name($opts->{db_name} = $database) if $database;

        $hostpairs = 'localhost' unless $hostpairs;
        @pairs =  map { $_ .= ':27017' unless $_ =~ /:/ ; $_ } split ',', $hostpairs;

        # TODO handle standard options from $options
    }
    # deprecated syntax
    else {
        push @pairs, $self->host.":".$self->port;
    }

    # a simple single server is special-cased (so we don't recurse forever)
    if (@pairs == 1 && !$self->find_master) {
        my @hp = split ":", $pairs[0];

        $self->_init_conn($hp[0], $hp[1], $self->ssl);
        if ($self->auto_connect) {
            $self->connect;
            $self->max_bson_size($self->_get_max_bson_size);
        }
        return;
    }

    # multiple servers
    my $connected = 0;
    $opts->{find_master} = 0;
    $opts->{auto_connect} = 0;
    foreach (@pairs) {
        $opts->{host} = "mongodb://$_";

        $self->_servers->{$_} = MongoDB::MongoClient->new($opts);

        next unless $self->auto_connect;

        # it's okay if we can't connect, so long as someone can
        eval {
            $self->_servers->{$_}->connect;
            $self->_servers->{$_}->max_bson_size($self->_servers->{$_}->_get_max_bson_size);
        };

        # at least one connection worked
        if (!$@) {
            $connected = 1;
        }
    }

    my $master;

    if ($self->auto_connect) {

        # if we still aren't connected to anyone, give up
        if (!$connected) {
            die "couldn't connect to any servers listed: ".join(",", @pairs);
        }

        $master = $self->get_master;
        if ($master == -1) {
            die "couldn't find master";
        }
        else {
            $self->max_bson_size($master->max_bson_size);
        }
    }
    else {
        # no auto-connect so just pick one. if auto-reconnect is set then it will connect as needed
        ($master) = values %{$self->_servers};
    }

    # create a struct that just points to the master's connection
    $self->_init_conn_holder($master);
}

sub _get_max_bson_size {
    my $self = shift;
    my $buildinfo = $self->get_database('admin')->run_command({buildinfo => 1});
    if (ref($buildinfo) eq 'HASH' && exists $buildinfo->{'maxBsonObjectSize'}) {
        return $buildinfo->{'maxBsonObjectSize'};
    }
    # default: 4MB
    return 4194304;
}


sub database_names {
    my ($self) = @_;
    my $ret = $self->get_database('admin')->run_command({ listDatabases => 1 });
    if (ref($ret) eq 'HASH' && exists $ret->{databases}) {
        return map { $_->{name} } @{ $ret->{databases} };
    }
    else {
        die ($ret);
    }
}

sub get_database {
    my ($self, $database_name) = @_;
    return MongoDB::Database->new(
        _client     => $self,
        name        => $database_name,
    );
}

sub _get_a_specific_connection {
    my ($self, $host) = @_;

    if ($self->_servers->{$host}->connected) {
        return $self->_servers->{$host};
    }

    eval {
        $self->_servers->{$host}->connect;
    };

    if (!$@) {
        return $self->_servers->{$host};
    }
    return 0;
}

sub _get_any_connection {
    my ($self) = @_;

    while ((my $key, my $value) = each(%{$self->_servers})) {
        my $conn = $self->_get_a_specific_connection($key);
        if ($conn) {
            return $conn;
        }
    }

    return 0;
}


sub get_master {
    my ($self) = @_;

    my $conn = $self->_get_any_connection();
    # if we couldn't connect to anything, just return
    if (!$conn) {
        return -1;
    }

    # a single server or list of servers
    if (!$self->find_master) {
        $self->_master($conn);
        return $self->_master;
    }
    # auto-detect master
    else {
        my $master = $conn->get_database($self->db_name)->run_command({"ismaster" => 1});

        # check for errors
        if (ref($master) eq 'SCALAR') {
            return -1;
        }

        # if this is a replica set & we haven't renewed the host list in 1 sec
        if ($master->{'hosts'} && time() > $self->ts) {
            # update (or set) rs list
            my %opts = ( auto_connect => 0 );
            if ($self->username && $self->password) {
                $opts{username} = $self->username;
                $opts{password} = $self->password;
                $opts{db_name}  = $self->db_name;
            }
            for (@{$master->{'hosts'}}) {
                if (!$self->_servers->{$_}) {
                    $self->_servers->{$_} = MongoDB::MongoClient->new("host" => "mongodb://$_", %opts);
                }
            }
            $self->ts(time());
        }

        # if this is the master, whether or not it's a replica set, return it
        if ($master->{'ismaster'}) {
            $self->_master($conn);
            return $self->_master;
        }
        elsif ($self->find_master && exists $master->{'primary'}) {
            my $primary = $self->_get_a_specific_connection($master->{'primary'});
            if (!$primary) {
                return -1;
            }

            # double-check that this is master
            my $result = $primary->get_database("admin")->run_command({"ismaster" => 1});
            if ($result->{'ismaster'}) {
                $self->_master($primary);
                return $self->_master;
            }
        }
    }

    return -1;
}


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


sub fsync {
    my ($self, $args) = @_;
	
	$args ||= {};

    # Pass this in as array-ref to ensure that 'fsync => 1' is the first argument.
    return $self->get_database('admin')->run_command([fsync => 1, %$args]);
}

sub fsync_unlock { 
    my ($self) = @_;
	
    # Have to fetch from a special collection to unlock.
    return $self->get_database('admin')->get_collection('$cmd.sys.unlock')->find_one();
}

sub _w_want_safe { 
    my ( $self ) = @_;

    my $w = $self->w;

    return 0 if $w =~ /^-?\d+$/ && $w <= 0;
    return 1;
}

sub _sasl_check { 
    my ( $self, $res ) = @_;

    die "Invalid SASL response document from server:"
        unless reftype $res eq reftype { };

    if ( $res->{ok} != 1 ) { 
        die "SASL authentication error: $res->{errmsg}";
    }

    return $res->{conversationId};
}

sub _sasl_start { 
    my ( $self, $payload, $mechanism ) = @_;

    # warn "SASL start, payload = [$payload], mechanism = [$mechanism]\n";

    my $res = $self->get_database( '$external' )->run_command( [ 
        saslStart     => 1,
        mechanism     => $mechanism,
        payload       => $payload,
        autoAuthorize => 1 ] );

    $self->_sasl_check( $res );
    return $res;
}


sub _sasl_continue { 
    my ( $self, $payload, $conv_id ) = @_;

    # warn "SASL continue, payload = [$payload], conv ID = [$conv_id]";

    my $res = $self->get_database( '$external' )->run_command( [ 
        saslContinue     => 1,
        conversationId   => $conv_id,
        payload          => $payload
    ] );

    $self->_sasl_check( $res );
    return $res;
}


sub _sasl_plain_authenticate { 
    my ( $self ) = @_;

    my $username = defined $self->username ? $self->username : "";
    my $password = defined $self->password ? $self->password : ""; 

    my $auth_bytes = encode( "UTF-8", "\x00" . $username . "\x00" . $password );
    my $payload = MongoDB::BSON::Binary->new( data => $auth_bytes ); 

    $self->_sasl_start( $payload, "PLAIN" );    
} 

__PACKAGE__->meta->make_immutable( inline_destructor => 0 );

1;



__END__

=pod

=head1 SYNOPSIS

The MongoDB::MongoClient class creates a client connection to the MongoDB server.

By default, it connects to a single server running on the local machine
listening on the default port:

    # connects to localhost:27017
    my $client = MongoDB::MongoClient->new;

It can connect to a database server running anywhere, though:

    my $client = MongoDB::MongoClient->new(host => 'example.com:12345');

See the L</"host"> section for more options for connecting to MongoDB.

=head1 MULTITHREADING

Cloning instances of this class is disabled in Perl 5.8.7+, so forked threads
will have to create their own connections to the database.

=head1 SEE ALSO

Core documentation on connections: L<http://docs.mongodb.org/manual/reference/connection-string/>.

=attr host

Server or servers to connect to. Defaults to C<mongodb://localhost:27017>.

To connect to more than one database server, use the format:

    mongodb://host1[:port1][,host2[:port2],...[,hostN[:portN]]]

An arbitrary number of hosts can be specified.

The connect method will return success if it can connect to at least one of the
hosts listed.  If it cannot connect to any hosts, it will die.

If a port is not specified for a given host, it will default to 27017. For
example, to connecting to C<localhost:27017> and C<localhost:27018>:

    my $client = MongoDB::MongoClient->new("host" => "mongodb://localhost,localhost:27018");

This will succeed if either C<localhost:27017> or C<localhost:27018> are available.

The connect method will also try to determine who is the primary if more than one
server is given.  It will try the hosts in order from left to right.  As soon as
one of the hosts reports that it is the primary, the connect will return success.  If
no hosts report themselves as a primary, the connect will die.

If username and password are given, success is conditional on being able to log
into the database as well as connect.  By default, the driver will attempt to
authenticate with the admin database.  If a different database is specified
using the C<db_name> property, it will be used instead.

=attr w

The client I<write concern>. 

=over 4

=item * C<-1> Errors ignored. Do not use this.

=item * C<0> Unacknowledged. MongoClient will B<NOT> wait for an acknowledgment that 
the server has received and processed the request. Older documentation may refer
to this as "fire-and-forget" mode. You must call C<getLastError> manually to check
if a request succeeds. This option is not recommended.

=item * C<1> Acknowledged. This is the default. MongoClient will wait until the 
primary MongoDB acknowledges the write.

=item * C<2> Replica acknowledged. MongoClient will wait until at least two 
replicas (primary and one secondary) acknowledge the write. You can set a higher 
number for more replicas.

=item * C<all> All replicas acknowledged.

=item * C<majority> A majority of replicas acknowledged.

=back

In MongoDB v2.0+, you can "tag" replica members. With "tagging" you can specify a 
new "getLastErrorMode" where you can create new
rules on how your data is replicated. To used you getLastErrorMode, you pass in the 
name of the mode to the C<w> parameter. For more infomation see: 
http://www.mongodb.org/display/DOCS/Data+Center+Awareness

=attr wtimeout

The number of milliseconds an operation should wait for C<w> slaves to replicate
it.

Defaults to 1000 (1 second).

See C<w> above for more information.

=attr j

If true, awaits the journal commit before returning. If the server is running without 
journaling, it returns immediately, and successfully.


=attr auto_reconnect

Boolean indicating whether or not to reconnect if the connection is
interrupted. Defaults to C<1>.

=attr auto_connect

Boolean indication whether or not to connect automatically on object
construction. Defaults to C<1>.

=attr timeout

Connection timeout in milliseconds. Defaults to C<20000>.

=attr username

Username for this client connection.  Optional.  If this and the password field are
set, the client will attempt to authenticate on connection/reconnection.

=attr password

Password for this connection.  Optional.  If this and the username field are
set, the client will attempt to authenticate on connection/reconnection.

=attr db_name

Database to authenticate on for this connection.  Optional.  If this, the
username, and the password fields are set, the client will attempt to
authenticate against this database on connection/reconnection.  Defaults to
"admin".

=attr query_timeout

    # set query timeout to 1 second
    my $client = MongoDB::MongoClient->new(query_timeout => 1000);

    # set query timeout to 6 seconds
    $client->query_timeout(6000);

This will cause all queries (including C<find_one>s and C<run_command>s) to die
after this period if the database has not responded.

This value is in milliseconds and defaults to the value of
L<MongoDB::Cursor/timeout>.

    $MongoDB::Cursor::timeout = 5000;
    # query timeout for $conn will be 5 seconds
    my $client = MongoDB::MongoClient->new;

A value of -1 will cause the driver to wait forever for responses and 0 will
cause it to die immediately.

This value overrides L<MongoDB::Cursor/timeout>.

    $MongoDB::Cursor::timeout = 1000;
    my $client = MongoDB::MongoClient->new(query_timeout => 10);
    # timeout for $conn is 10 milliseconds

=attr max_bson_size

This is the largest document, in bytes, storable by MongoDB. The driver queries
MongoDB on connection to determine this value.  It defaults to 4MB.

=attr find_master

If this is true, the driver will attempt to find a primary given the list of
hosts.  The primary-finding algorithm looks like:

    for host in hosts

        if host is the primary
             return host

        else if host is a replica set member
            primary := replica set's primary
            return primary

If no primary is found, the connection will fail.

If this is not set (or set to the default, 0), the driver will simply use the
first host in the host list for all connections.  This can be useful for
directly connecting to secondaries for reads.

If you are connecting to a secondary, you should read
L<MongoDB::Cursor/slave_okay>.

You can use the C<ismaster> command to find the members of a replica set:

    my $result = $db->run_command({ismaster => 1});

The primary and secondary hosts are listed in the C<hosts> field, the slaves are
in the C<passives> field, and arbiters are in the C<arbiters> field.

=attr ssl

This tells the driver that you are connecting to an SSL mongodb instance.

This option will be ignored if the driver was not compiled with the SSL flag. You must
also be using a database server that supports SSL.

The driver must be built as follows for SSL support:

    perl Makefile.PL --ssl
    make
    make install

Alternatively, you can set the C<PERL_MONGODB_WITH_SSL> environment variable before
installing:

    PERL_MONGODB_WITH_SSL=1 cpan MongoDB

The C<libcrypto> and C<libssl> libraries are required for SSL support.

=attr sasl

This attribute is experimental.

If set to C<1>, the driver will attempt to negotiate SASL authentication upon
connection. See L</sasl_mechanism> for a list of the currently supported mechanisms. The
driver must be built as follows for SASL support:

    perl Makefile.PL --sasl
    make
    make install

Alternatively, you can set the C<PERL_MONGODB_WITH_SASL> environment variable before
installing:

    PERL_MONGODB_WITH_SASL=1 cpan MongoDB

The C<libgsasl> library is required for SASL support. RedHat/CentOS users can find it
in the EPEL repositories.

Future versions of this driver may switch to L<Cyrus SASL|http://www.cyrusimap.org/docs/cyrus-sasl/2.1.25/>
in order to be consistent with the MongoDB server, which now uses Cyrus.

=attr sasl_mechanism

This attribute is experimental.

This specifies the SASL mechanism to use for authentication with a MongoDB server. (See L</sasl>.) 
The default is GSSAPI. The supported SASL mechanisms are:

=over 4

=item * C<GSSAPI>. This is the default. GSSAPI will attempt to authenticate against Kerberos
for MongoDB Enterprise 2.4+. You must run your program from within a C<kinit> session and set 
the C<username> attribute to the Kerberos principal name, e.g. C<user@EXAMPLE.COM>. 

=item * C<PLAIN>. The SASL PLAIN mechanism will attempt to authenticate against LDAP for
MongoDB Enterprise 2.6+. Because the password is not encrypted, you should only use this
mechanism over a secure connection. You must set the C<username> and C<password> attributes 
to your LDAP credentials.

=back

=attr dt_type

Sets the type of object which is returned for DateTime fields. The default is L<DateTime>. Other
acceptable values are L<DateTime::Tiny> and C<undef>. The latter will give you the raw epoch value
rather than an object.

=attr inflate_dbrefs

Controls whether L<DBRef|http://docs.mongodb.org/manual/applications/database-references/#dbref>s 
are automatically inflated into L<MongoDB::DBRef> objects. Defaults to true.
Set this to C<0> if you don't want to auto-inflate them.

=method connect

    $client->connect;

Connects to the MongoDB server. Called automatically on object construction if
L</auto_connect> is true.

=method database_names

    my @dbs = $client->database_names;

Lists all databases on the MongoDB server.

=method get_database($name)

    my $database = $client->get_database('foo');

Returns a L<MongoDB::Database> instance for the database with the given C<$name>.

=method get_master

    $master = $client->get_master

Determines which host of a paired connection is master.  Does nothing for
a non-paired connection.  This need never be invoked by a user, it is
called automatically by internal functions.  Returns the index of the master
connection in the list of connections or -1 if it cannot be determined.

=method authenticate ($dbname, $username, $password, $is_digest?)

    $client->authenticate('foo', 'username', 'secret');

Attempts to authenticate for use of the C<$dbname> database with C<$username>
and C<$password>. Passwords are expected to be cleartext and will be
automatically hashed before sending over the wire, unless C<$is_digest> is
true, which will assume you already did the hashing on yourself.

See also the core documentation on authentication:
L<http://docs.mongodb.org/manual/core/access-control/>.


=method send($str)

    my ($insert, $ids) = MongoDB::write_insert('foo.bar', [{name => "joe", age => 40}]);
    $client->send($insert);

Low-level function to send a string directly to the database.  Use
L<MongoDB::write_insert>, L<MongoDB::write_update>, L<MongoDB::write_remove>, or
L<MongoDB::write_query> to create a valid string.

=method recv(\%info)

    my $cursor = $client->recv({ns => "foo.bar"});

Low-level function to receive a response from the database. Returns a
C<MongoDB::Cursor>.  At the moment, the only required field for C<$info> is
"ns", although "request_id" is likely to be required in the future.  The
C<$info> hash will be automatically created for you by L<MongoDB::write_query>.


=method fsync(\%args)

    $client->fsync();

A function that will forces the server to flush all pending writes to the storage layer.

The fsync operation is synchronous by default, to run fsync asynchronously, use the following form:

    $client->fsync({async => 1});

The primary use of fsync is to lock the database during backup operations. This will flush all data to the data storage layer and block all write operations until you unlock the database. Note: you can still read while the database is locked. 

    $conn->fsync({lock => 1});

=method fsync_unlock

    $conn->fsync_unlock();

Unlocks a database server to allow writes and reverses the operation of a $conn->fsync({lock => 1}); operation. 
