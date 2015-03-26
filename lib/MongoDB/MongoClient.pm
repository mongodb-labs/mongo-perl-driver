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

# ABSTRACT: A connection to a MongoDB server or multi-server deployment

use version;
our $VERSION = 'v0.999.998.5'; # TRIAL

use Moose;
use MongoDB;
use MongoDB::Cursor;
use MongoDB::BSON::Binary;
use MongoDB::BSON::Regexp;
use MongoDB::Error;
use MongoDB::Op::_Command;
use MongoDB::ReadPreference;
use MongoDB::WriteConcern;
use MongoDB::_Topology;
use MongoDB::_Credential;
use MongoDB::_URI;
use Digest::MD5;
use Tie::IxHash;
use Time::HiRes qw/usleep/;
use Carp 'carp', 'croak';
use Safe::Isa;
use Scalar::Util 'reftype';
use Syntax::Keyword::Junction 'any';
use boolean;
use Encode;
use Try::Tiny;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

use constant {
    PRIMARY             => 'primary',
    SECONDARY           => 'secondary',
    PRIMARY_PREFERRED   => 'primaryPreferred',
    SECONDARY_PREFERRED => 'secondaryPreferred',
    NEAREST             => 'nearest',
};

#--------------------------------------------------------------------------#
# public attributes
#
# XXX too many of these are mutable
#--------------------------------------------------------------------------#

# connection attributes

=attr host

The C<host> attribute specifies either a single server to connect to (as
C<hostname> or C<hostname:port>), or else a L<connection string URI|/CONNECTION
STRING URI> with a seed list of one or more servers plus connection options.

Defaults to the connection string URI C<mongodb://localhost:27017>.

=cut

has host => (
    is      => 'ro',
    isa     => Str,
    default => 'mongodb://localhost:27017', # XXX eventually, make this localhost
);

=attr port

If a network port is not specified as part of the C<host> attribute, this
attribute provides the port to use.  It defaults to 27107.

=cut

has port => (
    is      => 'ro',
    isa     => Int,
    default => 27017,
);

=attr connect_type

Specifies the expected topology type of servers in the seed list.  The default
is 'none'.

Valid values include:

=for :list
* replicaSet – the topology is a replica set (ignore non replica set members
  during discovery)
* direct – the topology is a single server (connect as if the server is a
  standlone, even if it looks like a replica set member)
* none – discover the deployment topology by checking servers in the seed list
  and connect accordingly

=cut

has connect_type => (
    is      => 'ro',
    isa     => ConnectType,
    builder => '_build_connect_type',
    lazy    => 1
);

=attr timeout

Connection timeout in milliseconds. Defaults to C<20000>.

=cut

has timeout => (
    is      => 'rw',
    isa     => Int,
    default => 20000,
);


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

=cut

has query_timeout => (
    is      => 'rw',
    isa     => Int,
    default => sub { return $MongoDB::Cursor::timeout; },
);

=attr ssl

    ssl => 1
    ssl => \%ssl_options

This tells the driver that you are connecting to an SSL mongodb instance.

You must have L<IO::Socket::SSL> 1.42+ and L<Net::SSLeay> 1.49+ installed for
SSL support.

The C<ssl> attribute takes either a boolean value or a hash reference of
options to pass to IO::Socket::SSL.  For example, to set a CA file to validate
the server certificate and set a client certificate for the server to validate,
you could set the attribute like this:

    ssl => {
        SSL_ca_file   => "/path/to/ca.pem",
        SSL_cert_file => "/path/to/client.pem",
    }

If C<SSL_ca_file> is not provided, server certificates are verified against a
default list of CAs, either L<Mozilla::CA> or an operating-system-specific
default CA file.  To disable verification, you can use
C<< SSL_verify_mode => 0x00 >>.

B<You are strongly encouraged to use your own CA file for increased security>.

Server hostnames are also validated against the CN name in the server
certificate using C<< SSL_verifycn_scheme => 'default' >>.  You can use the
scheme 'none' to disable this check.

B<Disabling certificate or hostname verification is a security risk and is not
recommended>.

=cut

has ssl => (
    is      => 'ro',
    isa     => Bool|HashRef,
    default => 0,
    writer  => '_set_ssl',
);

# write concern attributes

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
name of the mode to the C<w> parameter. For more information see:
http://www.mongodb.org/display/DOCS/Data+Center+Awareness

=cut

has w => (
    is      => 'rw',
    isa     => Int|Str,
    default => 1,
    trigger => \&_update_write_concern,
);

=attr wtimeout

The number of milliseconds an operation should wait for C<w> slaves to replicate
it.

Defaults to 1000 (1 second).

See C<w> above for more information.

=cut

has wtimeout => (
    is      => 'rw',
    isa     => Int,
    default => 1000,
    trigger => \&_update_write_concern,
);

=attr j

If true, the client will block until write operations have been committed to the
server's journal. Prior to MongoDB 2.6, this option was ignored if the server was
running without journaling. Starting with MongoDB 2.6, write operations will fail
if this option is used when the server is running without journaling.

=cut

has j => (
    is      => 'rw',
    isa     => Bool,
    default => 0,
    trigger => \&_update_write_concern,
);

# server selection attributes

=attr server_selection_timeout_ms

This attribute specifies the amount of time in milliseconds to wait for a
suitable server to be available for a read or write operation.  If no
server is available within this time period, an exception will be thrown.

The default is 30,000 ms.

See L</SERVER SELECTION> for more details.

=cut

has server_selection_timeout_ms => (
    is      => 'ro',
    isa     => Num,
    default => 30_000,
);

=attr local_threshold_ms

The width of the 'latency window': when choosing between multiple suitable
servers for an operation, the acceptable delta in milliseconds between shortest
and longest average round-trip times.  Servers within the latency window are
selected randomly.

Set this to "0" to always select the server with the shortest average round
trip time.  Set this to a very high value to always randomly choose any known
server.

Defaults to 15 ms.

See L</SERVER SELECTION> for more details.

=cut

has local_threshold_ms => (
    is      => 'ro',
    isa     => Num,
    default => 15,
);

=attr read_preference

A L<MongoDB::ReadPreference> object, or a hash reference of attributes to
construct such an object.  The default is mode 'primary'.

For core documentation on read preference see
L<http://docs.mongodb.org/manual/core/read-preference/>.

B<The use of C<read_preference> as a mutator has been removed.>  Read
preference is read-only.  If you need a different read preference for
a database or collection, you can specify that in C<get_database> or
C<get_collection>.

=cut

has read_preference => (
    is        => 'ro',
    isa       => ReadPreference,
    writer    => '_set_read_preference',
    coerce    => 1,
    lazy      => 1,
    builder => '_build__read_preference',
);

sub _build__read_preference {
    my ($self) = @_;
    return MongoDB::ReadPreference->new;
}

# server monitoring

=attr heartbeat_frequency_ms

The time in milliseconds between scans of all servers to check if they
are up and update their latency.  Defaults to 60,000 ms.

=cut

has heartbeat_frequency_ms => (
    is      => 'ro',
    isa     => Num,
    default => 60_000,
);

# authentication attributes

=attr username

Optional username for this client connection.  If this field is set, the client
will attempt to authenticate when connecting to servers.  Depending on the
L</auth_mechanism>, the L</password> field or other attributes will need to be
set for authentication to succeed.

=cut

has username => (
    is      => 'rw',
    isa     => Str,
    lazy    => 1,
    builder => '_build_username',
);

=attr password

If an L</auth_mechanism> requires a password, this attribute will be
used.  Otherwise, it will be ignored.

=cut

has password => (
    is      => 'rw',
    isa     => Str,
    lazy    => 1,
    builder => '_build_password',
);

=attr db_name

Optional.  If an L</auth_mechanism> requires a database for authentication,
this attribute will be used.  Otherwise, it will be ignored. Defaults to
"admin".

=cut

has db_name => (
    is      => 'rw',
    isa     => Str,
    lazy    => 1,
    builder => '_build_db_name',
);

=attr auth_mechanism

This attribute determines how the client authenticates with the server.
Valid values are:

=for :list
* NONE
* DEFAULT
* MONGODB-CR
* MONGODB-X509
* GSSAPI
* PLAIN
* SCRAM-SHA-1

If not specified, then if no username is provided, it defaults to NONE.
If a username is provided, it is set to DEFAULT, which chooses SCRAM-SHA-1 if
available or MONGODB-CR otherwise.

=cut

has auth_mechanism => (
    is      => 'ro',
    isa     => AuthMechanism,
    lazy    => 1,
    builder => '_build_auth_mechanism',
    writer  => '_set_auth_mechanism',
);

=attr auth_mechanism_properties

This is an optional hash reference of authentication mechanism specific properties.
See L</AUTHENTICATION> for details.

=cut

has auth_mechanism_properties => (
    is      => 'ro',
    isa     => HashRef,
    lazy    => 1,
    builder => '_build_auth_mechanism_properties',
    writer  => '_set_auth_mechanism_properties',
);

# BSON conversion attributes

=attr dt_type

Sets the type of object which is returned for DateTime fields. The default is
L<DateTime>. Other acceptable values are L<DateTime::Tiny> and C<undef>. The
latter will give you the raw epoch value rather than an object.

=cut

has dt_type => (
    is      => 'rw',
    default => 'DateTime'
);


=attr inflate_dbrefs

Controls whether L<DBRef|http://docs.mongodb.org/manual/applications/database-references/#dbref>s
are automatically inflated into L<MongoDB::DBRef> objects. Defaults to true.
Set this to C<0> if you don't want to auto-inflate them.

=cut

has inflate_dbrefs => (
    is      => 'rw',
    isa     => Bool,
    default => 1
);

=attr inflate_regexps

Controls whether regular expressions stored in MongoDB are inflated into L<MongoDB::BSON::Regexp> objects instead of native Perl Regexps. The default is false. This can be dangerous, since the JavaScript regexps used internally by MongoDB are of a different dialect than Perl's. The default for this attribute may become true in future versions of the driver.

=cut

has inflate_regexps => (
    is      => 'rw',
    isa     => Bool,
    default => 0,
);

#--------------------------------------------------------------------------#
# deprecated public attributes
#--------------------------------------------------------------------------#

=attr auto_connect (DEPRECATED)

This attribute no longer has any effect.  Connections always connect on
demand.

=cut

has auto_connect => (
    is      => 'ro',
    isa     => Bool,
    default => 1,
);

=attr auto_reconnect (DEPRECATED)

This attribute no longer has any effect.  Connections always reconnect on
demand.

=cut

has auto_reconnect => (
    is      => 'ro',
    isa     => Bool,
    default => 1,
);


=attr find_master (DEPRECATED)

This attribute no longer has any effect.  The driver will always attempt
to find an appropriate server for every operation.

=cut

has find_master => (
    is      => 'ro',
    isa     => Bool,
    default => 0,
);

=attr sasl (DEPRECATED)

If true, the driver will set the authentication mechanism based on the
C<sasl_mechanism> property.

=cut

has sasl => (
    is      => 'ro',
    isa     => Bool,
    default => 0
);

=attr sasl_mechanism (DEPRECATED)

This specifies the SASL mechanism to use for authentication with a MongoDB server.
It has the same valid values as L</auth_mechanism>.  The default is GSSAPI.

=cut

has sasl_mechanism => (
    is      => 'ro',
    isa     => AuthMechanism,
    default => 'GSSAPI',
);

#--------------------------------------------------------------------------#
# private attributes
#--------------------------------------------------------------------------#

has _topology => (
    is         => 'ro',
    isa        => InstanceOf['MongoDB::_Topology'],
    lazy_build => 1,
    handles    => { topology_type => 'type' },
    clearer    => '_clear__topology',
);

has _credential => (
    is         => 'ro',
    isa        => InstanceOf['MongoDB::_Credential'],
    builder    => '_build__credential',
    lazy       => 1,
    writer     => '_set__credential',
);

has _min_wire_version => (
    is      => 'ro',
    isa     => Int,
    default => 0
);

has _max_wire_version => (
    is      => 'ro',
    isa     => Int,
    default => 3
);

has _uri => (
    is      => 'ro',
    isa     => InstanceOf['MongoDB::_URI'],
    lazy    => 1,
    builder => '_build__uri',
);

has _write_concern => (
    is     => 'ro',
    isa    => InstanceOf['MongoDB::WriteConcern'],
    writer => '_set_write_concern',
);

#--------------------------------------------------------------------------#
# builders
#--------------------------------------------------------------------------#

sub _build_auth_mechanism {
    my ($self) = @_;

    if ( $self->sasl ) {
        # XXX support deprecated legacy experimental API
        return $self->sasl_mechanism;
    }
    elsif ( my $mech = $self->_uri->options->{authMechanism} ) {
        return $mech;
    }
    elsif ( $self->username ) {
        return 'DEFAULT';
    }
    else {
        return 'NONE';
    }
}

sub _build_auth_mechanism_properties {
    my ($self) = @_;
    my $service_name = $self->_uri->options->{'authMechanism.SERVICE_NAME'};
    return {
        ( defined $service_name ? ( SERVICE_NAME => $service_name ) : () ),
    };
}

sub _build__topology {
    my ($self) = @_;

    my $type =
        $self->connect_type eq 'replicaSet' ? 'ReplicaSetNoPrimary'
      : $self->connect_type eq 'direct'     ? 'Single'
      :                                       'Unknown';

    MongoDB::_Topology->new(
        uri                         => $self->_uri,
        type                        => $type,
        server_selection_timeout_ms => $self->server_selection_timeout_ms,
        local_threshold_ms          => $self->local_threshold_ms,
        heartbeat_frequency_ms      => $self->heartbeat_frequency_ms,
        max_wire_version            => $self->_max_wire_version,
        min_wire_version            => $self->_min_wire_version,
        credential                  => $self->_credential,
        link_options                => {
            with_ssl   => !!$self->ssl,
            ( ref( $self->ssl ) eq 'HASH' ? ( SSL_options => $self->ssl ) : () ),
        },
    );
}

sub _build_connect_type {
    my ($self) = @_;
    return
      exists $self->_uri->options->{connect} ? $self->_uri->options->{connect} : 'none';
}

sub _build_db_name {
    my ($self) = @_;
    return $self->_uri->options->{authSource} || $self->_uri->db_name;
}

sub _build_password {
    my ($self) = @_;
    return $self->_uri->password;
}

sub _build_username {
    my ($self) = @_;
    return $self->_uri->username;
}

sub _build__credential {
    my ($self) = @_;
    my $mechanism = $self->auth_mechanism;
    my $cred = MongoDB::_Credential->new(
        mechanism            => $mechanism,
        mechanism_properties => $self->auth_mechanism_properties,
        ( $self->username ? ( username => $self->username ) : () ),
        ( $self->password ? ( password => $self->password ) : () ),
        ( $self->db_name  ? ( source   => $self->db_name )  : () ),
    );
    return $cred;
}

sub _build__uri {
    my ($self) = @_;
    if ( $self->host =~ m{^mongodb://} ) {
        return MongoDB::_URI->new( uri => $self->host );
    }
    else {
        my $uri = $self->host =~ /:\d+$/
                ? $self->host
                : sprintf("%s:%s", map { $self->$_ } qw/host port/ );
        return MongoDB::_URI->new( uri => ("mongodb://$uri") );
    }
}

sub BUILD {
    my ($self, $opts) = @_;

    my $uri = $self->_uri;

    my @addresses = @{ $uri->hostpairs };
    if ( $self->connect_type eq 'direct' && @addresses > 1 ) {
        MongoDB::UsageError->throw("Connect type 'direct' cannot be used with multiple addresses: @addresses");
    }

    my $options = $uri->options;

    # Add options from URI
    $self->_set_ssl(_str_to_bool($options->{ssl}))  if exists $options->{ssl};
    $self->timeout($options->{connectTimeoutMS})    if exists $options->{connectTimeoutMS};
    $self->w($options->{w})                         if exists $options->{w};
    $self->wtimeout($options->{wtimeoutMS})         if exists $options->{wtimeoutMS};
    $self->j(_str_to_bool($options->{journal}))     if exists $options->{journal};

    $self->_update_write_concern;

    if ( exists $options->{readPreference} ) {
        my $ts = $options->{readPreferenceTags};
        my $rp = MongoDB::ReadPreference->new(
            mode => $options->{readPreference},
            ( $ts ? ( tag_sets => $ts ) : () ),
        );
        $self->_set_read_preference($rp);
    }

    return;
}

#--------------------------------------------------------------------------#
# helper functions
#--------------------------------------------------------------------------#

sub _str_to_bool {
    my $str = shift;
    MongoDB::UsageError->throw("cannot convert undef to bool") unless defined $str;
    my $ret = $str eq "true" ? 1 : $str eq "false" ? 0 : undef;
    return $ret unless !defined $ret;
    MongoDB::UsageError->throw("expected boolean string 'true' or 'false' but instead received '$str'");
}

#--------------------------------------------------------------------------#
# public methods - network communication
#--------------------------------------------------------------------------#

=method connect

    $client->connect;

Calling this method is unnecessary, as connections are established
automatically as needed.  It is kept for backwards compatibility.  Calling it
will check all servers in the deployment which ensures a connection to any
that are available.

=cut

sub connect {
    my ($self) = @_;
    $self->_topology->scan_all_servers;
    return 1;
}

=method disconnect

    $client->disconnect;

Drops all connections to servers.

=cut

sub disconnect {
    my ($self) = @_;
    $self->_topology->close_all_links;
    return 1;
}

#--------------------------------------------------------------------------#
# semi-private methods; these are public but undocumented and their
# semantics might change in future releases
#--------------------------------------------------------------------------#

# Undocumented in old MongoDB::MongoClient; semantics don't translate, but
# best approximation is checking if we can talk to a primary
sub connected {
    my ($self) = @_;
    return try { !! $self->_topology->get_writable_link }
}

sub send_admin_command {
    my ( $self, $command, $read_preference ) = @_;

    my $op   = MongoDB::Op::_Command->new(
        db_name => 'admin',
        query => $command,
        ( $read_preference ? (read_preference => $read_preference) : ()),
    );

    return $self->send_read_op( $op );
}

sub send_direct_op {
    my ( $self, $op, $address ) = @_;
    my $link = $self->_topology->get_specific_link($address);
    return $self->_try_op( $op, $link );
}

sub send_write_op {
    my ( $self, $op ) = @_;
    my $link = $self->_topology->get_writable_link;
    return $self->_try_op( $op, $link );
}

sub send_read_op {
    my ( $self, $op ) = @_;
    my $link = $self->_topology->get_readable_link($op->read_preference);
    my $type = $self->_topology->type;
    return $self->_try_op( $op, $link, $type );
}

sub _try_op {
    # $type might be undef; not needed for writes
    my ($self, $op, $link, $type) = @_;

    my $result = try {
        $op->execute( $link, $type );
    }
    catch {
        if ( $_->$_isa("MongoDB::ConnectionError") ) {
            $self->_topology->mark_server_unknown( $link->server, $_ );
        }
        elsif ( $_->$_isa("MongoDB::NotMasterError") ) {
            $self->_topology->mark_server_unknown( $link->server, $_ );
            $self->_topology->mark_stale;
        }
        # regardless of cleanup, rethrow the error
        die $_;
    };

    return $result;
}

#--------------------------------------------------------------------------#
# write concern methods
#--------------------------------------------------------------------------#

sub _update_write_concern {
    my ($self) = @_;
    my $wc = MongoDB::WriteConcern->new(
        w        => $self->w,
        wtimeout => $self->wtimeout,
        ( $self->j ? ( j => $self->j ) : () ),
    );
    $self->_set_write_concern($wc);
}

#--------------------------------------------------------------------------#
# database helper methods
#--------------------------------------------------------------------------#

=method database_names

    my @dbs = $client->database_names;

Lists all databases on the MongoDB server.

=cut

sub database_names {
    my ($self) = @_;

    my @databases;
    my $max_tries = 3;
    for my $try ( 1 .. $max_tries ) {
        last if try {
            my $output = $self->send_admin_command([ listDatabases => 1 ])->output;
            if (ref($output) eq 'HASH' && exists $output->{databases}) {
                @databases = map { $_->{name} } @{ $output->{databases} };
            }
            return 1;
        } catch {
            # can't open db in a read lock
            return if $_->result->ouput->{code} == CANT_OPEN_DB_IN_READ_LOCK() || $try < $max_tries;
            die $_;
        };
    }

    return @databases;
}

=method get_database, db

    my $database = $client->get_database('foo');
    my $database = $client->get_database('foo', $options);
    my $database = $client->db('foo', $options);

Returns a L<MongoDB::Database> instance for the database with the given
C<$name>.

It takes an optional hash reference of options that are passed to the
L<MongoDB::Database> constructor.

The C<db> method is an alias for C<get_database>.

=cut

sub get_database {
    my ( $self, $database_name, $options ) = @_;
    return MongoDB::Database->new(
        read_preference => $self->read_preference,
        write_concern => $self->_write_concern,
        ( $options ? %$options : () ),
        # not allowed to be overridden by options
        _client       => $self,
        name          => $database_name,
    );
}

{ no warnings 'once'; *db = \&get_database }

=method get_namespace, ns

    my $collection = $client->get_namespace('test.foo');
    my $collection = $client->get_namespace('test.foo', $options);
    my $collection = $client->ns('test.foo', $options);

Returns a L<MongoDB::Collection> instance for the given namespace.
The namespace has both the database name and the collection name
separated with a dot character.

This is a quick way to get a collection object if you don't need
the database object separately.

It takes an optional hash reference of options that are passed to the
L<MongoDB::Collection> constructor.  The intermediate L<MongoDB::Database>
object will be created with default options.

The C<ns> method is an alias for C<get_namespace>.

=cut

sub get_namespace {
    my ( $self, $ns, $options ) = @_;
    MongoDB::UsageError->throw("namespace requires a string argument")
      unless defined($ns) && length($ns);
    my ( $db, $coll ) = split /\./, $ns, 2;
    MongoDB::UsageError->throw("$ns is not a valid namespace")
      unless defined($db) && defined($coll);
    return $self->db($db)->coll( $coll, $options );
}

{ no warnings 'once'; *ns = \&get_namespace }

=method fsync(\%args)

    $client->fsync();

A function that will forces the server to flush all pending writes to the storage layer.

The fsync operation is synchronous by default, to run fsync asynchronously, use the following form:

    $client->fsync({async => 1});

The primary use of fsync is to lock the database during backup operations. This will flush all data to the data storage layer and block all write operations until you unlock the database. Note: you can still read while the database is locked.

    $conn->fsync({lock => 1});

=cut

sub fsync {
    my ($self, $args) = @_;

    $args ||= {};

    # Pass this in as array-ref to ensure that 'fsync => 1' is the first argument.
    return $self->get_database('admin')->run_command([fsync => 1, %$args]);
}

=method fsync_unlock

    $conn->fsync_unlock();

Unlocks a database server to allow writes and reverses the operation of a $conn->fsync({lock => 1}); operation.

=cut

sub fsync_unlock {
    my ($self) = @_;

    # Have to fetch from a special collection to unlock.
    return $self->get_database('admin')->get_collection('$cmd.sys.unlock')->find_one();
}

=method authenticate (DEPRECATED)

    $client->authenticate($dbname, $username, $password, $is_digest);

B<This legacy method is deprecated but kept for backwards compatibility.>

Instead, authentication credentials should be provided as constructor arguments
or as part of the connection URI.

When C<authenticate> is called, it disconnects the client (if any connections
had been made), sets client attributes as if the username and password had been
used initially in the client constructor, and reconnects to the configured
servers.  The authentication mechanism will be MONGO-CR for servers before
version 3.0 and SCRAM-SHA-1 for 3.0 or later.

Passwords are expected to be cleartext and will be automatically hashed before
sending over the wire, unless C<$is_digest> is true, which will assume you
already did the proper hashing yourself.

See also the L</AUTHENTICATION> section.

=cut

sub authenticate {
    my ( $self, $db_name, $username, $password, $is_digest ) = @_;

    # set client properties
    $self->_set_auth_mechanism('DEFAULT');
    $self->_set_auth_mechanism_properties( {} );
    $self->db_name($db_name);
    $self->username($username);
    $self->password($password);

    my $cred = MongoDB::_Credential->new(
        mechanism            => $self->auth_mechanism,
        mechanism_properties => $self->auth_mechanism_properties,
        username             => $self->username,
        password             => $self->password,
        source               => $self->db_name,
        pw_is_digest         => $is_digest,
    );
    $self->_set__credential($cred);

    # ensure that we've authenticated by clearing the topology and trying a
    # command that opens a socket
    $self->_clear__topology;
    $self->send_admin_command( { ismaster => 1 } );

    return 1;
}

__PACKAGE__->meta->make_immutable( inline_destructor => 0 );

1;


__END__

=pod

=for Pod::Coverage
connected
send_admin_command
send_direct_op
send_read_op
send_write_op

=head1 SYNOPSIS

    use MongoDB; # also loads MongoDB::MongoClient

    # connect to localhost:27017
    my $client = MongoDB::MongoClient->new;

    # connect to specific host and port
    my $client = MongoDB::MongoClient->new(
        host => "mongodb://mongo.example.com:27017"
    );

    my $db = $client->get_database("test");
    my $coll = $db->get_collection("people");

    $coll->insert({ name => "John Doe", age => 42 });
    my @people = $coll->find()->all();

=head1 DESCRIPTION

The C<MongoDB::MongoClient> class represents a client connection to one or
more MongoDB servers.

By default, it connects to a single server running on the local machine
listening on the default port 27017:

    # connects to localhost:27017
    my $client = MongoDB::MongoClient->new;

It can connect to a database server running anywhere, though:

    my $client = MongoDB::MongoClient->new(host => 'example.com:12345');

See the L</"host"> attribute for more options for connecting to MongoDB.

MongoDB can be started in L<authentication
mode|http://docs.mongodb.org/manual/core/authentication/>, which requires
clients to log in before manipulating data.  By default, MongoDB does not start
in this mode, so no username or password is required to make a fully functional
connection.  To configure the client for authentication, see the
L</AUTHENTICATION> section.

The actual socket connections are lazy and created on demand.  When the client
object goes out of scope, all socket will be closed.  Note that
L<MongoDB::Database>, L<MongoDB::Collection> and related classes could hold a
reference to the client as well.  Only when all references are out of scope
will the sockets be closed.

=head1 DEPLOYMENT TOPOLOGY

MongoDB can operate as a single server or as a distributed system.  One or more
servers that collectively provide access to a single logical set of MongoDB
databases are referred to as a "deployment".

There are three types of deployments:

=for :list
* Single server – a stand-alone mongod database
* Replica set – a set of mongod databases with data replication and fail-over
  capability
* Sharded cluster – a distributed deployment that spreads data across one or
  more shards, each of which can be a replica set.  Clients communicate with
  a mongos process that routes operations to the correct share.

The state of a deployment, including its type, which servers are members, the
server types of members and the round-trip network latency to members is
referred to as the "topology" of the deployment.

To the greatest extent possible, the MongoDB driver abstracts away the details
of communicating with different deployment types.  It determines the deployment
topology through a combination of the connection string, configuration options
and direct discovery communicating with servers in the deployment.

=head1 CONNECTION STRING URI

MongoDB uses a pseudo-URI connection string to specify one or more servers to
connect to, along with configuration options.

To connect to more than one database server, provide host or host:port pairs
as a comma separated list:

    mongodb://host1[:port1][,host2[:port2],...[,hostN[:portN]]]

This list is referred to as the "seed list".  An arbitrary number of hosts can
be specified.  If a port is not specified for a given host, it will default to
27017.

If multiple hosts are given in the seed list or discovered by talking to
servers in the seed list, they must all be replica set members or must all be
mongos servers for a sharded cluster.

If a single, non-replica-set server is found, or if the L</connect_type> (or
C<connect> URI option) is 'direct', the deployment is treated as a single
server deployment.  For a replica set member, forcing the connection type to be
'direct' routes all operations to it alone; this is useful for carrying out
administrative activities on that server.

The connection string may also have a username and password:

    mongodb://username:password@host1:port1,host2:port2

The username and password must be URL-escaped.

A optional database name for authentication may be given:

    mongodb://username:password@host1:port1,host2:port2/my_database

Finally, connection string options may be given as URI attribute pairs in a query
string:

    mongodb://host1:port1,host2:port2/?ssl=1&wtimeoutMS=1000
    mongodb://username:password@host1:port1,host2:port2/my_database?ssl=1&wtimeoutMS=1000

The currently supported connection string options are:

=for :list
*authMechanism
*authMechanism.SERVICE_NAME
*connect
*connectTimeoutMS
*journal
*readPreference
*readPreferenceTags
*ssl
*w
*wtimeoutMS

See the official MongoDB documentation on connection strings for more on the URI
format and connection string options:
L<http://docs.mongodb.org/manual/reference/connection-string/>.

=head1 SERVER SELECTION

For a single server deployment or a direct connection to a mongod or mongos, all
reads and writes and sent to that server.  Any read-preference is ignored.

When connected to a deployment with multiple servers, such as a replica set or
sharded cluster, the driver chooses a server for operations based on the type of
operation (read or write), the types of servers available and a read preference.

For a replica set deployment, writes are sent to the primary (if available) and
reads are sent to a server based on the L</read_preference> attribute, which default
to sending reads to the primary.  See L<MongoDB::ReadPreference> for more.

For a sharded cluster reads and writes are distributed across mongos servers in
the seed list.  Any read preference is passed through to the mongos and used
by it when executing reads against shards.

If multiple servers can service an operation (e.g. multiple mongos servers,
or multiple replica set members), one is chosen at random from within the
"latency window".  The server with the shortest average round-trip time (RTT)
is always in the window.  Any servers with an average round-trip time less than
or equal to the shortest RTT plus the L</local_threshold_ms> are also in the
latency window.

If a server is not immediately available, the driver will block for up to
L</server_selection_timeout_ms> milliseconds waiting for a suitable server to
become available.  If no server is available at the end of that time, an
exception is thrown.

=head1 SERVER MONITORING

When the client first needs to find a server for a database operation, all
servers from the L</host> attribute are scanned to determine which servers to
monitor.  If the deployment is a replica set, additional hosts may be
discovered in this process.  Invalid hosts are dropped.

After the initial scan, whenever the servers have not been checked in
L</heartbeat_frequency_ms> milliseconds, the scan will be repeated.  This
amortizes monitoring time over many of operations.

Additionally, if a socket has been idle for a while, it will be checked
before being used for an operation.

If an server operation fails because of a "not master" or "node is recovering"
error, the server is flagged as unavailable.  Assuming the error is caught and
handled, the next operation will rescan all servers immediately to find a new
primary.

Whenever a server is found to be unavailable, the driver records this fact, but
can continue to function as long as other servers are suitable per L</SERVER
SELECTION>.

=head1 AUTHENTICATION

The MongoDB server provides several authentication mechanisms, though some
are only available in the Enterprise edition.

MongoDB client authentication is controlled via the L</auth_mechanism>
attribute, which takes one of the following values:

=for :list
* MONGODB-CR -- legacy username-password challenge-response
* SCRAM-SHA-1 -- secure username-password challenge-response (3.0+)
* MONGODB-X509 -- SSL client certificate authentication (2.6+)
* PLAIN -- LDAP authentication via SASL PLAIN (Enterprise only)
* GSSAPI -- Kerberos authentication (Enterprise only)

The mechanism to use depends on the authentication configuration of the
server.  See the core documentation on authentication:
L<http://docs.mongodb.org/manual/core/access-control/>.

Usage information for each mechanism is given below.

=head2 MONGODB-CR and SCRAM-SHA-1 (for username/password)

These mechnisms require a username and password, given either as
constructor attributes or in the C<host> connection string.

If a username is provided and an authentication mechanism is not specified,
the client will use SCRAM-SHA-1 for version 3.0 or later servers and will
fall back to MONGODB-CR for older servers.

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://mongo.example.com/",
        username => "johndoe",
        password => "trustno1",
    );

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://johndoe:trustno1@mongo.example.com/",
    );

Usernames and passwords will be UTF-8 encoded before use.  The password is
never sent over the wire -- only a secure digest is used.  The SCRAM-SHA-1
mechanism is the Salted Challenge Response Authentication Mechanism
definedin L<RFC 5802|http://tools.ietf.org/html/rfc5802>.

The default database for authentication is 'admin'.  If another database
name should be used, specify it with the C<db_name> attribute or via the
connection string.

    db_name => auth_db

    mongodb://johndoe:trustno1@mongo.example.com/auth_db

=head2 MONGODB-X509 (for SSL client certificate)

X509 authentication requires SSL support (L<IO::Socket::SSL>) and requires
that a client certificate be configured and that the username attribute be
set to the "Subject" field, formatted according to RFC 2253.  To find the
correct username, run the C<openssl> program as follows:

  $ openssl x509 -in certs/client.pem -inform PEM -subject -nameopt RFC2253
  subject= CN=XXXXXXXXXXX,OU=XXXXXXXX,O=XXXXXXX,ST=XXXXXXXXXX,C=XX

In this case the C<username> attribute would be
C<CN=XXXXXXXXXXX,OU=XXXXXXXX,O=XXXXXXX,ST=XXXXXXXXXX,C=XX>.

Configure your client with the correct username and ssl parameters, and
specify the "MONGODB-X509" authentication mechanism.

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://sslmongo.example.com/",
        ssl => {
            SSL_ca_file   => "certs/ca.pem",
            SSL_cert_file => "certs/client.pem",
        },
        auth_mechanism => "MONGODB-X509",
        username       => "CN=XXXXXXXXXXX,OU=XXXXXXXX,O=XXXXXXX,ST=XXXXXXXXXX,C=XX"
    );

=head2 PLAIN (for LDAP)

This mechanism requires a username and password, which will be UTF-8
encoded before use.  The C<auth_mechanism> parameter must be given as a
constructor attribute or in the C<host> connection string:

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://mongo.example.com/",
        username => "johndoe",
        password => "trustno1",
        auth_mechanism => "PLAIN",
    );

    my $mc = MongoDB::MongoClient->new(
        host => "mongodb://johndoe:trustno1@mongo.example.com/authMechanism=PLAIN",
    );

=head2 GSSAPI (for Kerberos)

Kerberos authentication requires the CPAN module L<Authen::SASL> and a
GSSAPI-capable backend.

On Debian systems, L<Authen::SASL> may be available as
C<libauthen-sasl-perl>; on RHEL systems, it may be available as
C<perl-Authen-SASL>.

The L<Authen::SASL::Perl> backend comes with L<Authen::SASL> and requires
the L<GSSAPI> CPAN module for GSSAPI support.  On Debian systems, this may
be available as C<libgssapi-perl>; on RHEL systems, it may be available as
C<perl-GSSAPI>.

Installing the L<GSSAPI> module from CPAN rather than an OS package
requires C<libkrb5> and the C<krb5-config> utility (available for
Debian/RHEL systems in the C<libkrb5-dev> package).

Alternatively, the L<Authen::SASL::XS> or L<Authen::SASL::Cyrus> modules
may be used.  Both rely on Cyrus C<libsasl>.  L<Authen::SASL::XS> is
preferred, but not yet available as an OS package.  L<Authen::SASL::Cyrus>
is available on Debian as C<libauthen-sasl-cyrus-perl> and on RHEL as
C<perl-Authen-SASL-Cyrus>.

Installing L<Authen::SASL::XS> or L<Authen::SASL::Cyrus> from CPAM requires
C<libsasl>.  On Debian systems, it is available from C<libsasl2-dev>; on
RHEL, it is available in C<cyrus-sasl-devel>.

To use the GSSAPI mechanism, first run C<kinit> to authenticate with the ticket
granting service:

    $ kinit johndoe@EXAMPLE.COM

Configure MongoDB::MongoClient with the principal name as the C<username>
parameter and specify 'GSSAPI' as the C<auth_mechanism>:

    my $mc = MongoDB::MongoClient->new(
        host => 'mongodb://mongo.example.com',
        username => 'johndoe@EXAMPLE.COM',
        auth_mechanism => 'GSSAPI',
    );

Both can be specified in the C<host> connection string, keeping in mind
that the '@' in the principal name must be encoded as "%40":

    my $mc = MongoDB::MongoClient->new(
        host =>
          'mongodb://johndoe%40EXAMPLE.COM@mongo.examplecom/?authMechanism=GSSAPI',
    );

The default service name is 'mongodb'.  It can be changed with the
C<auth_mechanism_properties> attribute or in the connection string.

    auth_mechanism_properties => { SERVICE_NAME => 'other_service' }

    mongodb://.../?authMechanism=GSSAPI&authMechanism.SERVICE_NAME=other_service


=head1 THREAD-SAFETY AND FORK-SAFETY

Existing connections to servers are closed after forking or spawning a thread.  They
will reconnect on demand.

=cut

