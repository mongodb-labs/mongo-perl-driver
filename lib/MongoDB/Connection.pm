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
# ABSTRACT: A connection to a Mongo server

use Any::Moose;

=attr host

Hostname to connect to. Defaults to C<loalhost>.

=cut

has host => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'localhost',
);

=attr port

Port to use when connecting. Defaults to C<27017>.

=cut

has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017,
);

=attr left_host

Paired connection host to connect to. Can be master or slave.

=cut

has left_host => (
    is       => 'ro',
    isa      => 'Str',
);

=attr left_port

Port to use when connecting to left_host. Defaults to C<27017>.

=cut

has left_port => (
    is       => 'ro',
    isa      => 'Int',
    default  => 27017,
);

=attr right_host

Paired connection host to connect to. Can be master or slave.

=cut

has right_host => (
    is       => 'ro',
    isa      => 'Str',
);

=attr right_port

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

=attr auto_reconnect

Boolean indicating whether or not to reconnect if the connection is
interrupted. Defaults to C<0>.

=cut

has auto_reconnect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0,
);

=attr auto_connect

Boolean indication whether or not to connect automatically on object
construction. Defaults to C<1>.

=cut

has auto_connect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 1,
);

has _database_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'MongoDB::Database',
);

has _cursor_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'MongoDB::Cursor',
);

has _oid_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'MongoDB::OID',
);

sub _build__server {
    my ($self) = @_;
    my ($host, $port) = map { $self->$_ } qw/host port/;
    return "${host}:${port}";
}

sub BUILD {
    my ($self) = @_;
    eval "use ${_}" # no Any::Moose::load_class becase the namespaces already have symbols from the xs bootstrap
        for map { $self->$_ } qw/_database_class _cursor_class _oid_class/;
    $self->connect if $self->auto_connect;
}

=method connect

    $connection->connect;

Connects to the mongo server. Called automatically on object construction if
C<auto_connect> is true.

=cut

sub find_one {
    my ($self, $ns, $query) = @_;
    $query ||= {};
    return $self->_find_one($ns, $query);
}

sub query {
    my ($self, $ns, $query, $attrs) = @_;
    my ($limit, $skip, $sort_by) = @{ $attrs || {} }{qw/limit skip sort_by/};
    $limit   ||= 0;
    $skip    ||= 0;
    return $self->_query($ns, $query, $limit, $skip, $sort_by);
}

sub insert {
    my ($self, $ns, $object) = @_;
    confess 'not a hash reference' unless ref $object eq 'HASH';
    my %copy = %{ $object }; # a shallow copy is good enough. we won't modify anything deep down in the structure.
    $copy{_id} = $self->_oid_class->new unless exists $copy{_id};
    $self->_insert($ns, \%copy);
    return $copy{_id};
}

sub update {
    my ($self, $ns, $query, $object, $upsert) = @_;
    $upsert = 0 unless defined $upsert;
    $self->_update($ns, $query, $object, $upsert);
    return;
}

sub remove {
    my ($self, $ns, $query) = @_;
    $self->_remove($ns, $query, 0);
    return;
}

{
    my %direction_map = (
        ascending  => 1,
        descending => -1,
    );

    sub ensure_index {
        my ($self, $ns, $keys, $direction, $unique) = @_;
        $direction ||= 'ascending';
        $unique = 0 unless defined $unique;

        my %keys;
        if (ref $keys eq 'ARRAY') {
            %keys = map { ($_ => $direction) } @{ $keys };
        }
        elsif (ref $keys eq 'HASH') {
            %keys = %{ $keys };
        }
        else {
            confess 'expected hash or array reference for keys';
        }

        my $k = { map {
            my $dir = $keys{$_};
            confess "unknown direction '${dir}'"
                unless exists $direction_map{$dir};
            ($_ => $direction_map{$dir})
        } keys %keys };

        my @name;
        while ((my $idx, my $d) = each(%$k)) {
            push @name, $idx;
            push @name, $d;
        }

        my $obj = {"ns" => $ns,
                   "key" => $k,
                   "name" => join("_", @name)};

        $self->_ensure_index(substr($ns, 0, index($ns, ".")).".system.indexes", $obj, $unique);
        return;
    }
}

=method database_names

    my @dbs = $connection->database_names;

Lists all databases on the mongo server.

=cut

sub database_names {
    my ($self) = @_;
    my $ret = $self->get_database('admin')->run_command({ listDatabases => 1 });
    return map { $_->{name} } @{ $ret->{databases} };
}

=method get_database ($name)

    my $database = $connection->get_database('foo');

Returns a C<MongoDB::Database> instance for database with the given C<$name>.

=cut

sub get_database {
    my ($self, $database_name) = @_;
    return $self->_database_class->new(
        _connection => $self,
        name        => $database_name,
    );
}

=method find_master

    $connection->find_master

Determines which host of a paired connection is master.  Does nothing for
a non-paired connection.  Called automatically by internal functions.

=cut

sub find_master {
    my ($self) = @_;
    return unless defined $self->left_host && $self->right_host;

    my $left = MongoDB::Connection->new("host" => $self->left_host, "port" => $self->left_port);
    my $master = $left->find_one('admin.$cmd', {ismaster => 1});
    if ($master->{'ismaster'}) {    
        return 0;
    }

    my $right = MongoDB::Connection->new("host" => $self->right_host, "port" => $self->right_port);
    $master = $right->find_one('admin.$cmd', {ismaster => 1});
    if ($master->{'ismaster'}) {
        return 1;
    }

    # something went wrong
    croak("couldn't find master");
}

=method authenticate ($dbname, $username, $password, $is_digest?)

    $connection->authenticate('foo', 'username', 'secret');

Attempts to authenticate for use of the C<$dbname> database with C<$username>
and C<$password>. Passwords are expected to be cleartext and will be
automatically hashed before sending over the wire, unless C<$is_digest> is
true, which will assume you already did the hashing on yourself.

=cut

sub authenticate {
    my ($self, @args) = @_;
    return $self->_authenticate(@args);
}

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
