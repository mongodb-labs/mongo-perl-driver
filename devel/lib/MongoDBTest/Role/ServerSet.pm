#  Copyright 2009-2014 MongoDB, Inc.
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

use 5.010;
use strict;
use warnings;

package MongoDBTest::Role::ServerSet;

use MongoDBTest::Mongod;
use MongoDBTest::Mongos;

use Moo::Role;
use Types::Standard -types;
use namespace::clean;

# To be satisfied by consumer

requires '_logger';

# Required

has default_args => (
    is => 'rwp',
    isa => Str,
    required => 1,
);

has default_version => (
    is => 'ro',
    isa => Str,
    required => 1,
);

has timeout => (
    is => 'ro',
    isa => Maybe[Num],
);

has auth_config => (
    is => 'ro',
    isa => Maybe[HashRef],
);

has server_config_list => (
    is => 'ro',
    isa => ArrayRef[HashRef],
    required => 1,
);

# Default

has server_type => (
    is => 'ro',
    isa => Enum[qw/mongod mongos/],
    default => 'mongod',
);

# Private

has _servers => (
    is => 'lazy',
    isa => HashRef,
);

sub _build__servers {
    my ($self) = @_;
    my $set = {};
    for my $server ( @{ $self->server_config_list } ) {
        my $class = "MongoDBTest::" . ucfirst( $self->server_type );
        $set->{$server->{name}} = $class->new(
            config => $server,
            default_args => $self->default_args,
            default_version => $self->default_version,
            auth_config => $self->auth_config,
            ( $self->timeout ? ( timeout => $self->timeout ) : () ),
        );
    }
    return $set;
}

# Methods

sub all_servers { 
    my ($self) = @_;
    return sort { $a->name cmp $b->name } values %{ $self->_servers }
}

sub get_server {
    my ($self, $name) = @_;
    for my $server ( $self->all_servers ) {
        return $server if $name eq $server->name;
    }
    return;
}

sub start {
    my ($self) = @_;
    # XXX eventually factor out wait_port from server->start, start all servers
    # and wait in a loop for them all to be up
    for my $server ( $self->all_servers ) {
        my $name = $server->name;
        $self->_logger->info("Starting $name");
        $server->start;
        $self->_logger->info("Server $name is up on port " . $server->port);
    }
    return;
}

sub stop {
    my ($self) = @_;
    for my $server ( $self->all_servers ) {
        next unless $server->is_alive;
        my $name = $server->name;
        $self->_logger->info("stopping $name");
        $server->stop;
    }
    return;
}

sub as_uri {
    my ($self) = @_;
    my $uri = "mongodb://" . $self->as_pairs;
    if ( $self->auth_config ) {
        my ($u,$p) = @{$self->auth_config}{qw/user password/};
        $uri =~ s{mongodb://}{mongodb://$u:$p\@};
    }
    return $uri;
}

sub as_pairs {
    my ($self) = @_;
    return join(",", map { $_->hostname . ":" . $_->port } $self->all_servers);
}

sub DEMOLISH {
    my ($self) = @_;
    $self->stop;
}

1;
