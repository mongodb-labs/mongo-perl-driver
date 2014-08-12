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

use version;
our $VERSION = 'v0.704.4.1';

use Moose;
use MongoDB;
use MongoDB::Cursor;
use MongoDB::BSON::Binary;
use MongoDB::BSON::Regexp;
use MongoDB::Error;
use Digest::MD5;
use Tie::IxHash;
use Time::HiRes qw/usleep/;
use Carp 'carp', 'croak';
use Scalar::Util 'reftype';
use boolean;
use Encode;
use Try::Tiny;
use MongoDB::_Types;
use namespace::clean -except => 'meta';

use constant {
    PRIMARY             => 0, 
    SECONDARY           => 1,
    PRIMARY_PREFERRED   => 2,
    SECONDARY_PREFERRED => 3,
    NEAREST             => 4 
};

use constant _READPREF_MODENAMES => ['primary',
                                     'secondary',
                                     'primaryPreferred',
                                     'secondaryPreferred',
                                     'nearest'];

use constant {
    MIN_HEARTBEAT_FREQUENCY_MS => 10,
    MAX_SCAN_TIME_SEC => 60,
};

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


has _readpref_mode => (
    is      => 'rw',
    isa     => 'Str',
    default => MongoDB::MongoClient->PRIMARY
);

has _readpref_tagsets => (
    is       => 'rw',
    isa      => 'ArrayRef',
    required => 0
);

has _readpref_pinned => (
    is       => 'rw',
    isa      => 'MongoDB::MongoClient',
    required => 0
);

has _readpref_retries => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 3
);

has _readpref_pingfreq_sec => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 5 
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
    is       => 'rw',
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

# XXX this really shouldn't be required -- it should be populated lazily
# on each connect (and probably private, too!)
has max_bson_size => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => 4194304,
);

has _max_bson_wire_size => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => 16_793_600, # 16MiB + 16KiB
);

# XXX eventually, get this off an isMaster call
has _max_write_batch_size => (
    is       => 'rw',
    isa      => 'Int',
    required => 1,
    default  => 1000,
);

has find_master => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0,
);

has _is_mongos => (
    is       => 'rw',
    isa      => 'Bool',
    required => 1,
    default  => 0
);

has ssl => (
    is       => 'rw',
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
    isa      => 'SASLMech',
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
    required => 0,
);

# cache our original constructor args in BUILD for creating
# new, per-host connections
has _opts => (
    is      => 'rw',
    isa     => 'HashRef',
    default => sub { {} },
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

has inflate_regexps => ( 
    is        => 'rw',
    isa       => 'Bool',
    required  => 0,
    default   => 0,
);

# attributes for keeping track of client and server wire protocol versions
has min_wire_version => ( 
    is        => 'ro',
    isa       => 'Int',
    required  => 1,
    default   => 0
);

has max_wire_version => (
    is        => 'ro',
    isa       => 'Int',
    required  => 1,
    default   => 2
);

has _use_write_cmd => ( 
    is        => 'ro',
    isa       => 'Bool',
    required  => 1,
    lazy_build => 1
);

has _link => (
    is        => 'rw', # XXX rw to proxy to master connection
    isa       => 'MongoDB::_Link',
    lazy_build => 1,
    handles => {
        connected => 'connected',
        send => 'write',
        recv => 'read',
    },
);

has _conn_params => (
    is  => 'rw',
    isa => 'ArrayRef',
);

sub BUILD {
    my ($self, $opts) = @_;
    eval "use ${_}" # no Any::Moose::load_class because the namespaces already have symbols from the xs bootstrap
        for qw/MongoDB::Database MongoDB::Cursor MongoDB::OID MongoDB::Timestamp/;

    my @pairs;

    my %parsed_connection = _parse_connection_string($self->host);

    # supported syntax (see http://docs.mongodb.org/manual/reference/connection-string/)
    if (%parsed_connection) {

        @pairs = @{$parsed_connection{hostpairs}};

        # we add these things to $opts as well as self so that they get propagated when we recurse for multiple servers
        for my $k ( qw/username password db_name/ ) {
            $self->$k($opts->{$k} = $parsed_connection{$k}) if exists $parsed_connection{$k};
        }

        # Process options
        my %options = %{$parsed_connection{options}} if defined $parsed_connection{options};

        # Add connection options
        $self->ssl($opts->{ssl} = _str_to_bool($options{ssl})) if exists $options{ssl};
        $self->timeout($opts->{timeout} = $options{connectTimeoutMS}) if exists $options{connectTimeoutMS};

        # Add write concern options
        $self->w($opts->{w} = $options{w}) if exists $options{w};
        $self->wtimeout($opts->{wtimeout} = $options{wtimeoutMS}) if exists $options{wtimeoutMS};
        $self->j($opts->{j} = _str_to_bool($options{journal})) if exists $options{journal};
    }
    # deprecated syntax
    else {
        push @pairs, $self->host.":".$self->port;
    }

    # We cache our updated constructor arguments because we need them again for
    # creating new, per-host objects
    $self->_opts( $opts );

    # a simple single server is special-cased (so we don't recurse forever)
    if (@pairs == 1 && !$self->find_master) {
        my @hp = split ":", $pairs[0];

        $self->_init_conn($hp[0], $hp[1], $self->ssl);
        if ($self->auto_connect) {
            $self->connect;
        }
        return;
    }

    # multiple servers
    my $first_server;
    my $connected = 0;
    my %errors;
    for my $pair (@pairs) {

        # override host, find_master and auto_connect
        my $args = {
            %$opts,
            host => "mongodb://$pair",
            find_master => 0,
            auto_connect => 0,
        };

        $self->_servers->{$pair} = MongoDB::MongoClient->new($args);
        $first_server = $self->_servers->{$pair} unless defined $first_server;

        next unless $self->auto_connect;

        # it's okay if we can't connect, so long as someone can
        eval {
            $self->_servers->{$pair}->connect;
        };

        # at least one connection worked
        if (!$@) {
            $connected = 1;
        }
        else {
            $errors{$pair} = $@;
            $errors{$pair} =~ s/at \S+ line \d+.*//;
        }
    }

    my $master;

    if ($self->auto_connect) {

        # if we still aren't connected to anyone, give up
        if (!$connected) {
            die "couldn't connect to any servers listed:\n" . join("", map { "$_: $errors{$_}" } keys %errors );
        }

        $master = $self->get_master($first_server);
        $self->max_bson_size($master->max_bson_size);
    }
    else {
        # no auto-connect so just pick one. if auto-reconnect is set then it will connect as needed
        $master = $first_server;
    }

    # user master's link directly
    # XXX alternatively, we could store master and proxy all send/recv through it.  Eventually,
    # the common architecture will replace this with a cluster/node abstraction and we won't
    # use MongoClient to play so many roles at the same time.
    $self->_link($master->_link);
}

sub _str_to_bool {
    my $str = shift;
    confess "cannot convert undef to bool" unless defined $str;
    my $ret = $str eq "true" ? 1 : $str eq "false" ? 0 : undef;
    return $ret unless !defined $ret;
    confess "expected boolean string 'true' or 'false' but instead received '$str'";
}

sub _unescape_all {
    my $str = shift;
    $str =~ s/%([0-9a-f]{2})/chr(hex($1))/ieg;
    return $str;
}

sub _parse_connection_string {

    my ($host) = @_;
    my %result;

    if ($host =~ m{ ^
            mongodb://
            (?: ([^:]*) : ([^@]*) @ )? # [username:password@]
            ([^/]*) # host1[:port1][,host2[:port2],...[,hostN[:portN]]]
            (?:
               / ([^?]*) # /[database]
                (?: [?] (.*) )? # [?options]
            )?
            $ }x ) {

        ($result{username}, $result{password}, $result{hostpairs}, $result{db_name}, $result{options}) = ($1, $2, $3, $4, $5);

        # Decode components
        for my $subcomponent ( qw/username password db_name/ ) {
            $result{$subcomponent} = _unescape_all($result{$subcomponent}) unless !(defined $result{$subcomponent});
        }

        $result{hostpairs} = 'localhost' unless $result{hostpairs};
        $result{hostpairs} = [
            map { @_ = split ':', $_; _unescape_all($_[0]).":"._unescape_all($_[1]) }
            map { $_ .= ':27017' unless $_ =~ /:/ ; $_ } split ',', $result{hostpairs}
        ];

        $result{options} =
            { map {
                 my @kv = split '=', $_;
                 confess 'expected key value pair' unless @kv == 2;
                 ($kv[0], $kv[1]) = (_unescape_all($kv[0]), _unescape_all($kv[1]));
                 @kv;
              } split '&', $result{options}
            } if defined $result{options};

        delete $result{username} unless defined $result{username} && length $result{username};
        delete $result{password} unless defined $result{password}; # can be empty string
        delete $result{db_name} unless defined $result{db_name} && length $result{db_name};
    }

    return %result;
}

sub _update_server_attributes {
    my ($self) = @_;
    $self->max_bson_size($self->_get_max_bson_size);
    $self->_check_wire_version;
}

sub _build__use_write_cmd { 
    my $self = shift;

    # find out if we support write commands
    my $result = eval {
        $self->get_database( $self->db_name )->_try_run_command( { "ismaster" => 1 } );
    };

    my $max_wire_version = ($result && exists $result->{maxWireVersion} )
        ? $result->{maxWireVersion} : 0;

    return 1 if $max_wire_version > 1;
    return 0;
}

sub _build__link {
    my ($self) = @_;
    # XXX eventually add SSL CA parameters
    return MongoDB::_Link->new(
        timeout => $self->timeout,
        reconnect => $self->auto_reconnect,
    );
}

sub _init_conn {
    my ($self, @params) = @_;
    $self->_conn_params( \@params );
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

sub connect {
    my ($self) = @_;
    return $self->_link->connect(@{$self->_conn_params});
}

sub disconnect {
    my ($self) = @_;
    my $link = $self->_link;
    $link->close if defined $link;
}

sub database_names {
    my ($self) = @_;

    my @databases;
    my $max_tries = 3;
    for my $try ( 1 .. $max_tries ) {
        last if try {
            my $result = $self->get_database('admin')->_try_run_command({ listDatabases => 1 });
            if (ref($result) eq 'HASH' && exists $result->{databases}) {
                @databases = map { $_->{name} } @{ $result->{databases} };
            }
            return 1;
        } catch {
            # can't open db in a read lock
            return if $_->{result}->{result}{code} == CANT_OPEN_DB_IN_READ_LOCK() || $try < $max_tries;
            die $_;
        };
    }

    return @databases;
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
    return;
}

sub _get_any_connection {
    my ($self) = @_;

    if ( ! keys %{$self->_servers} ) {
        return $self;
    }

    while ((my $key, my $value) = each(%{$self->_servers})) {
        my $conn = $self->_get_a_specific_connection($key);
        if ($conn) {
            # force a reset of the iterator 
            my $reset = keys %{$self->_servers};
            return $conn;
        }
    }

    return;
}

sub get_master {
    my ($self, $conn) = @_;

    my $start = time;
    while ( time - $start < MAX_SCAN_TIME_SEC ) {
        $conn ||= $self->_get_any_connection()
            or next;

        # a single server or list of servers
        if (!$self->find_master) {
            $self->_master($conn);
            return $self->_master;
        }
        # auto-detect master
        else {
            my $master = try {
                $conn->get_database($self->db_name)->_try_run_command({"ismaster" => 1})
            };

            if ( !$master ) { 
                undef $conn;
                next;
            };

            # msg field from ismaster command will
            # be set if in a sharded environment 
            $self->_is_mongos(1) if $master->{'msg'};

            # if this is a replica set & list of hosts is different, then update
            if ($master->{'hosts'}
                && join("", sort @{$master->{hosts}}) ne join("",sort keys %{$self->_servers})
            ) {

                # clear old host list before refreshing
                %{$self->_servers} = ();

                for (@{$master->{'hosts'}}) {
                    # override host, find_master and auto_connect
                    my $args = {
                        %{ $self->_opts },
                        host => "mongodb://$_",
                        find_master => 0,
                        auto_connect => 0,
                    };

                    $self->_servers->{$_} = $_ eq $master->{me} ? $conn : MongoDB::MongoClient->new($args);
                }
            }

            # if this is the master, whether or not it's a replica set, return it
            if ($master->{'ismaster'}) {
                $self->_master($conn);
                return $self->_master;
            }
            elsif ($self->find_master && exists $master->{'primary'}) {
                my $primary = $self->_get_a_specific_connection($master->{'primary'})
                    or next;

                # double-check that this is master
                my $result = try {
                    $primary->get_database("admin")->_try_run_command({"ismaster" => 1})
                };

                if ( ! $result ) {
                    $conn = $primary;
                    next;
                };

                if ($result && $result->{'ismaster'}) {
                    $self->_master($primary);
                    return $self->_master;
                }
            }
        }
    }
    continue {
        usleep(MIN_HEARTBEAT_FREQUENCY_MS);
    }

    confess "couldn't find master";
}


sub read_preference {
    my ($self, $mode, $tagsets) = @_;

    croak "Missing read preference mode" if @_ < 2;
    croak "Unrecognized read preference mode: $mode" if $mode < 0 || $mode > 4;
    croak "NEAREST read preference mode not supported" if $mode == MongoDB::MongoClient->NEAREST; 
    if (!$self->_is_mongos && (!$self->find_master || keys %{$self->_servers} < 2)) {
        croak "Read preference must be used with a replica set; is find_master false?";
    }
    croak "PRIMARY cannot be combined with tags" if $mode == MongoDB::MongoClient->PRIMARY && $tagsets;

    # only repin if mode or tagsets have changed
    return if $mode == $self->_readpref_mode &&
              defined $self->_readpref_tagsets &&
              defined $tagsets &&
              $tagsets == $self->_readpref_tagsets;

    $self->_readpref_mode($mode);

    $self->_readpref_tagsets($tagsets) if defined $tagsets;
    $self->_readpref_tagsets([]) if !(defined $tagsets);

    $self->repin();
}

sub _choose_secondary {
    my ($self, $servers) = @_;

    for (1 .. $self->_readpref_retries) {

        my @secondaries = keys %{$servers};
        return undef if @secondaries == 0;

        my $secondary = $servers->{$secondaries[int(rand(scalar @secondaries))]};

        if ($secondary->_check_ok(1)) {
            return $secondary;
        }
        else {
            delete $servers->{$secondary->host};
        }
    }

    return undef;
}


sub _narrow_by_tagsets {
    my ($self, $servers) = @_;

    return unless @{$self->_readpref_tagsets};

    my $conn = $self->_get_any_connection();
    if (!$conn) {
        # no connections available, clear the hash
        undef %{$servers};
        return;
    }

    my $replcoll = $conn->get_database('local')->get_collection('system.replset');
    my $rsconf = $replcoll->find_one();

    foreach my $conf (@{$rsconf->{'members'}}) {
        next unless exists $conf->{'tags'};

        my $member_matches = 0;

        # see if any of the tagsets match the rs conf
        TAGSET:
        foreach my $tagset (@{$self->_readpref_tagsets}) {

            foreach my $tagkey (keys %{$tagset}) {
                next TAGSET unless exists $conf->{'tags'}->{$tagkey} &&
                                   $tagset->{$tagkey} eq $conf->{'tags'}->{$tagkey};
            }

            $member_matches = 1;
        }

        # eliminate non-matching RS members
        delete $servers->{'mongodb://' . $conf->{'host'}} unless $member_matches;
    }
}

sub _check_ok {
    my ($self, $retries) = @_;

    foreach (1 .. $retries) {

        my $status = try {
            $self->get_database('admin')->_try_run_command({ping => 1});
        };

        return 1 if $status;
    }

    return 0;
}

sub repin {
    my ($self) = @_;

    if ($self->_is_mongos) {
        $self->_readpref_pinned($self);
        return;
    }

    $self->get_master if !$self->_master;

    my %secondaries = %{$self->_servers};
    foreach (keys %secondaries) {
        my $value = $secondaries{$_};
        $value->{'query_timeout'} = $self->query_timeout;
        delete $secondaries{$_};
        $secondaries{"mongodb://$_"} = $value;
    }
    my $primary = $secondaries{$self->_master->host};
    confess "internal error in host list" unless $primary;
    delete $secondaries{$primary->host};

    my $mode = $self->_readpref_mode;

    # pin the primary or die
    if ($mode == MongoDB::MongoClient->PRIMARY) {
        if ($primary->_check_ok($self->_readpref_retries)) {
            $self->_readpref_pinned($primary);
            return;
        }
        else {
            die "No replica set primary available for query with read_preference PRIMARY";
        }
    }

    # pin an arbitrary secondary or die
    elsif ($mode == MongoDB::MongoClient->SECONDARY) {
        $self->_narrow_by_tagsets(\%secondaries);
        my $secondary = $self->_choose_secondary(\%secondaries);
        if ($secondary) {
            $self->_readpref_pinned($secondary);
            return;
        }
        else {
            die "No replica set secondary available for query with read_preference SECONDARY";
        }
    }

    # if no primary available, then pin an arbitrary secondary
    elsif ($mode == MongoDB::MongoClient->PRIMARY_PREFERRED) {
        if ($primary->_check_ok($self->_readpref_retries)) {
            $self->_readpref_pinned($primary);
            return;
        }
        else {
            $self->_narrow_by_tagsets(\%secondaries);
            my $secondary = $self->_choose_secondary(\%secondaries);
            if ($secondary) {
                $self->_readpref_pinned($secondary);
                return;
            }
        }
    }

    # if no secondary available, then pin the primary
    elsif ($mode == MongoDB::MongoClient->SECONDARY_PREFERRED) {
        $self->_narrow_by_tagsets(\%secondaries);
        my $secondary = $self->_choose_secondary(\%secondaries);
        if ($secondary) {
            $self->_readpref_pinned($secondary);
            return;
        }
        elsif ($primary->_check_ok($self->_readpref_retries)) {
            $self->_readpref_pinned($primary);
            return;
        }
    }

    die "No replica set members available for query";
}


sub rs_refresh {
    my ($self) = @_;

    # only refresh if connected directly
    # to a replica set
    return unless $self->find_master;
    return unless $self->_readpref_pinned;
    return if $self->_is_mongos;

    # ping rs members, and repin if something has changed
    my $repin_required = 0;
    my $any_conn;
    if (time() > ($self->ts + $self->_readpref_pingfreq_sec)) {
        for (keys %{$self->_servers}) {
            my $server = $self->_servers->{$_};
            my $connected = $server->connected;
            my $ok = $server->_check_ok(1);
            if (($ok && !$connected) || (!$ok && $connected)) {
                $repin_required = 1;
            }
            if ($ok && !$any_conn) {
                $any_conn = $server;
            }
        }
        $self->get_master($any_conn) if $any_conn;
    }
   
    $self->repin if $repin_required;
    $self->ts(time());
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
    my $result = eval { $db->_try_run_command({getnonce => 1}) };
    if (!$result) {
        return $@
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


sub _check_wire_version { 
    my ( $self ) = @_;
    # check our wire protocol version compatibility
    
    my $master = $self->get_database( $self->db_name )->_try_run_command( { ismaster => 1 } );
    $master->{minWireVersion} ||= 0;
    $master->{maxWireVersion} ||= 0;

    if (    ( $master->{minWireVersion} > $self->max_wire_version )
            or ( $master->{maxWireVersion} < $self->min_wire_version ) ) { 
        die "Incompatible wire protocol version. This version of the MongoDB driver is not compatible with the server. You probably need to upgrade this library.";
    }

}

sub _write_concern {
    my ($self) = @_;
    my $wc = {
        w => $self->w,
        wtimeout => $self->wtimeout,
    };
    $wc->{j} = $self->j if $self->j;
    return $wc;
}

sub DESTROY {
    my ($self) = @_;
    $self->disconnect;
}

__PACKAGE__->meta->make_immutable( inline_destructor => 0 );

1;



__END__

=pod

=head1 SYNOPSIS

    use strict;
    use warnings;
    use MongoDB;

    # connects to localhost:27017
    my $client = MongoDB::MongoClient->new;

    my $db = $client->get_database("test");

=head1 DESCRIPTION

The C<MongoDB::MongoClient> class creates a client connection to one or
more MongoDB servers.

By default, it connects to a single server running on the local machine
listening on the default port 27017:

    # connects to localhost:27017
    my $client = MongoDB::MongoClient->new;

It can connect to a database server running anywhere, though:

    my $client = MongoDB::MongoClient->new(host => 'example.com:12345');

See the L</"host"> section for more options for connecting to MongoDB.

MongoDB can be started in I<authentication mode>, which requires clients to log in
before manipulating data.  By default, MongoDB does not start in this mode, so no
username or password is required to make a fully functional connection.  If you
would like to learn more about authentication, see the C<authenticate> method.

Connecting is relatively expensive, so try not to open superfluous connections.

There is no way to explicitly disconnect from the database.  However, the
connection will automatically be closed and cleaned up when no references to
the C<MongoDB::MongoClient> object exist, which occurs when C<$client> goes out of
scope (or earlier if you undefine it with C<undef>).

=head1 MULTITHREADING

Existing connections are closed when a thread is created.  If C<auto_reconnect>
is true, then connections will be re-established as needed.

=head1 SEE ALSO

Core documentation on connections: L<http://docs.mongodb.org/manual/reference/connection-string/>.

The currently supported connection string options are ssl, connectTimeoutMS, w, wtimeoutMS, and journal.

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
name of the mode to the C<w> parameter. For more information see: 
http://www.mongodb.org/display/DOCS/Data+Center+Awareness

=attr wtimeout

The number of milliseconds an operation should wait for C<w> slaves to replicate
it.

Defaults to 1000 (1 second).

See C<w> above for more information.

=attr j

If true, the client will block until write operations have been committed to the
server's journal. Prior to MongoDB 2.6, this option was ignored if the server was 
running without journaling. Starting with MongoDB 2.6, write operations will fail 
if this option is used when the server is running without journaling.

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

=attr inflate_regexps

Controls whether regular expressions stored in MongoDB are inflated into L<MongoDB::BSON::Regexp> objects instead of native Perl Regexps. The default is false. This can be dangerous, since the JavaScript regexps used internally by MongoDB are of a different dialect than Perl's. The default for this attribute may become true in future versions of the driver. 

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

=method recv($cursor)

    my $ok = $client->recv($cursor);

Low-level function to receive a response from the database into a cursor.
Dies on error.  Returns true if any results were received and false otherwise.

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

=method read_preference

    $conn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED, [{'disk' => 'ssd'}, {'rack' => 'k'}]);

Sets the read preference for this connection. The first argument is the read
preference mode and should be one of four constants: PRIMARY, SECONDARY,
PRIMARY_PREFERRED, or SECONDARY_PREFERRED (NEAREST is not yet supported).
In order to use read preference, L<MongoDB::MongoClient/find_master> must be set.
The second argument (optional) is an array reference containing tagsets. The tagsets can
be used to match the tags for replica set secondaries. See also
L<MongoDB::Cursor/read_preference>. For core documentation on read preference
see L<http://docs.mongodb.org/manual/core/read-preference/>.

=method repin

    $conn->repin()

Chooses a replica set member to which this connection should route read operations,
according to the read preference that has been set via L<MongoDB::MongoClient/read_preference>
or L<MongoDB::Cursor/read_preference>. This method is called automatically
when the read preference or replica set state changes, and generally does not
need to be called by application code. 

=method rs_refresh

    $conn->rs_refresh()

If it has been at least 5 seconds since last checking replica set state,
then ping all replica set members. Calls L<MongoDB::MongoClient/repin> if
a previously reachable node is now unreachable, or a previously unreachable
node is now reachable. This method is called automatically before communicating
with the server, and therefore should not generally be called by client code.

