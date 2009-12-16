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
our $VERSION = '0.26';

# ABSTRACT: A connection to a Mongo server

use MongoDB;
use MongoDB::Cursor;

use Any::Moose;
use Digest::MD5;
use Tie::IxHash;
use boolean;

=head1 NAME

MongoDB::Connection - A connection to a Mongo server

=head1 VERSION

version 0.26

=head1 SYNOPSIS

The MongoDB::Connection class creates a connection to 
the MongoDB server. 

By default, it connects to a single server running on
the local machine listening on the default port:

    # connects to localhost:27017
    my $connection = MongoDB::Connection->new;

It can connect to a database server running anywhere, 
though:

    my $connection = MongoDB::Connection->new(host => 'example.com', port => 12345);

It can also be used to connect to a replication pair
of database servers:

    my $connection = MongoDB::Connection->new(left_host => '192.0.2.0', right_host => '192.0.2.1');

If ports aren't given, they default to C<27017>.


=head1 ATTRIBUTES

=head2 host

Hostname to connect to. Defaults to C<localhost>.

=cut

has host => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'localhost',
);

=head2 port

Port to use when connecting. Defaults to C<27017>.

=cut

has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017,
);

=head2 left_host

Paired connection host to connect to. Can be master or slave.

=cut

has left_host => (
    is       => 'ro',
    isa      => 'Str',
);

=head2 left_port

Port to use when connecting to left_host. Defaults to C<27017>.

=cut

has left_port => (
    is       => 'ro',
    isa      => 'Int',
    default  => 27017,
);

=head2 right_host

Paired connection host to connect to. Can be master or slave.

=cut

has right_host => (
    is       => 'ro',
    isa      => 'Str',
);

=head2 right_port

Port to use when connecting to right_host. Defaults to C<27017>.

=cut

has right_port => (
    is       => 'ro',
    isa      => 'Int',
    default  => 27017,
);

has _server => (
    is       => 'ro',
    isa      => 'Str',
    lazy     => 1,
    builder  => '_build__server',
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


sub _build__server {
    my ($self) = @_;
    my ($host, $port) = map { $self->$_ } qw/host port/;
    return "${host}:${port}";
}

sub BUILD {
    my ($self) = @_;
    eval "use ${_}" # no Any::Moose::load_class becase the namespaces already have symbols from the xs bootstrap
        for qw/MongoDB::Database MongoDB::Cursor MongoDB::OID/;
    $self->connect if $self->auto_connect;
}

=head1 METHODS

=head2 connect

    $connection->connect;

Connects to the mongo server. Called automatically on object construction if
C<auto_connect> is true.

=cut

sub find_one {
    my ($self, $ns, $query, $fields) = @_;
    $query ||= {};
    $fields ||= {};
    return $self->query($ns, $query)->limit(-1)->fields($fields)->next;
}

sub query {
    my ($self, $ns, $query, $attrs) = @_;
    my ($limit, $skip, $sort_by) = @{ $attrs || {} }{qw/limit skip sort_by/};
    $limit   ||= 0;
    $skip    ||= 0;

    my $q = {};
    if ($sort_by) {
        $q->{'query'} = $query;
	$q->{'orderby'} = $sort_by;
    }
    else {
        $q = $query ? $query : {};
    }

    my $cursor = MongoDB::Cursor->new(
	_connection => $self,
	_ns => $ns, 
	_query => $q, 
	_limit => $limit, 
	_skip => $skip
    );
    $cursor->_init;
    return $cursor;
}

sub insert {
    my ($self, $ns, $object, $options) = @_;
    my @id = $self->batch_insert($ns, [$object], $options);
    return $id[0];
}

sub batch_insert {
    my ($self, $ns, $object, $options) = @_;
    confess 'not an array reference' unless ref $object eq 'ARRAY';

    my ($insert, $ilen, $ids) = MongoDB::write_insert($ns, $object);

    if (defined($options) && $options->{safe}) {
        my ($db, $coll) = $ns =~ m/^([^\.]+)\.(.*)/;
        my ($query, $qlen, $info) = write_query($db.'$cmd', 0, 0, -1, {getlasterror => 1});

        $self->send("$insert$query");

        my $cursor = $self->recv($info);
        my $ok = $cursor->next();
        if (!$ok->{ok}) {
            die $ok->{err};
        }
    }
    else {
        $self->send($insert);
    }

    return @$ids;
}

sub update {
    my ($self, $ns, $query, $object, $opts) = @_;

    # there used to be one option: upsert=0/1
    # now there are two, there will probably be
    # more in the future.  So, to support old code,
    # passing "1" will still be supported, but not
    # documentd, so we can phase that out eventually.
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

    $self->_update($ns, $query, $object, $flags);
    return;
}

sub remove {
    my ($self, $ns, $query, $just_one) = @_;
    $query ||= {};
    $just_one ||= 0;
    $self->_remove($ns, $query, $just_one);
    return;
}

{
    my %direction_map = (
        ascending  => 1,
        descending => -1,
    );


    # arg, this is such a mess.  support fade out:
    #     .27 - support for old & new format
    #     .28 - support for new format, remove documentation on old format
    #     .29 - remove old format
    sub _old_ensure_index {
        my ($self, $ns, $keys, $direction, $unique) = @_;
        $direction ||= 'ascending';
        $unique = 0 unless defined $unique;

        my $k;
        if (ref $keys eq 'ARRAY' ||
            ref $keys eq 'HASH' ) {
            my %keys;
            if (ref $keys eq 'ARRAY') {
                %keys = map { ($_ => $direction) } @{ $keys };
            }
            else {
                %keys = %{ $keys };
            }

            $k = { map {
                my $dir = $keys{$_};
                confess "unknown direction '${dir}'"
                    unless exists $direction_map{$dir};
                ($_ => $direction_map{$dir})
            } keys %keys };
        }
        elsif (ref $keys eq 'Tie::IxHash') {
            my @ks = $keys->Keys;
            my @vs = $keys->Values;

            for (my $i=0; $i<$keys->Length; $i++) {
                $keys->Replace($i, $direction_map{$vs[$i]});
            }

            $k = $keys;
        }
        else {
            confess 'expected Tie::IxHash, hash, or array reference for keys';
        }

        my @name = MongoDB::Collection::to_index_string($k);
        my $obj = {"ns" => $ns,
                   "key" => $k,
                   "name" => join("_", @name),
                   "unique" => $unique ? boolean::true : boolean::false};
        
        my ($db, $coll) = $ns =~ m/^([^\.]+)\.(.*)/;
        $self->insert("$db.system.indexes", $obj);
        return;
    }

    sub ensure_index {
        my ($self, $ns, $keys, $options, $garbage) = @_;

        # we need to use the crappy old api if...
        #  - $options isn't a hash, it's a string like "ascending"
        #  - $keys is a one-element array: [foo]
        #  - $keys is an array with more than one element and the second 
        #    element isn't a direction (or at least a good one)
        #  - Tie::IxHash has values like "ascending"
        if (($options && ref $options ne 'HASH') ||
            (ref $keys eq 'ARRAY' && 
             ($#$keys == 0 || $#$keys >= 1 && !($keys->[1] =~ /-?1/))) ||
            (ref $keys eq 'Tie::IxHash' && $keys->[2][0] =~ /(de)|(a)scending/)) {
            _old_ensure_index(@_);
            return;
        }

        my $obj = Tie::IxHash->new("ns" => $ns, 
            "key" => $keys, 
            "name" => MongoDB::Collection::to_index_string($keys));

        if (exists $options->{unique}) {
            $obj->Push("unique" => ($options->{unique} ? boolean::true : boolean::false));
        }
        if (exists $options->{drop_dups}) {
            $obj->Push("dropDups" => ($options->{drop_dups} ? boolean::true : boolean::false));
        }

        my ($db, $coll) = $ns =~ m/^([^\.]+)\.(.*)/;
        $self->insert("$db.system.indexes", $obj);
        return;
    }
}

=head2 database_names

    my @dbs = $connection->database_names;

Lists all databases on the mongo server.

=cut

sub database_names {
    my ($self) = @_;
    my $ret = $self->get_database('admin')->run_command({ listDatabases => 1 });
    return map { $_->{name} } @{ $ret->{databases} };
}

=head2 get_database ($name)

    my $database = $connection->get_database('foo');

Returns a C<MongoDB::Database> instance for database with the given C<$name>.

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
called automatically by internal functions.  Returns values:

=over

=item 0 

The left host is master

=item 1

The right host is master

=item -1 

Error, master cannot be determined.

=back

=cut

sub find_master {
    my ($self) = @_;
    # return if the connection isn't paired
    return unless defined $self->left_host && $self->right_host;
    my ($left, $right, $master);

    # check the left host
    eval {
        $left = MongoDB::Connection->new("host" => $self->left_host, "port" => $self->left_port);
    };
    if (!($@ =~ m/couldn't connect to server/)) {
        $master = $left->find_one('admin.$cmd', {ismaster => 1});
        if ($master->{'ismaster'}) {    
            return 0;
        }
    }

    # check the right_host
    eval {
        $right = MongoDB::Connection->new("host" => $self->right_host, "port" => $self->right_port);
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

    my ($insert, $len, $ids) = MongoDB::write_insert('foo.bar', [{name => "joe", age => 40}]);
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

sub recv {
    my ($self, $info) = @_;
    my $cursor = MongoDB::Cursor->new(_ns => $info->{ns}, _connection => $self, _query => {});
    $cursor->_init;
    $self->_recv($cursor);
    return $cursor;
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;

=head1 AUTHOR

  Kristina Chodorow <kristina@mongodb.org>
