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

use 5.010;
use strict;
use warnings;

package MongoDBTest::Orchestrator;

use MongoDBTest::Server;

use YAML::XS;

use Moo;
use Types::Standard qw/Str HashRef/;
use Types::Path::Tiny qw/AbsFile/;
use namespace::clean;

with 'MooseX::Role::Logger';

# Required

has config_file => (
    is => 'ro',
    isa => AbsFile,
    coerce => AbsFile->coercion,
    required => 1,
);

# Lazy or default

has config => (
    is => 'lazy',
    isa => HashRef,
);

sub _build_config {
    my ($self) = @_;
    my ($config) = YAML::XS::LoadFile($self->config_file);
    return $config;
}

has cluster_type => (
    is => 'lazy',
    isa => Str,
);

sub _build_cluster_type {
    my ($self) = @_;
    return $self->config->{type};
}

# Private

has _mongod_set => (
    is => 'ro',
    isa => HashRef,
    default => sub { {} },
);

# methods

sub BUILD {
    my ($self) = @_;

    for my $server ( @{ $self->config->{mongod} } ) {
        $self->_mongod_set->{$server->{name}} = MongoDBTest::Server->new(config => $server);
    }

    return;
}

sub list_mongod_set { 
    my ($self) = @_;
    return values %{ $self->_mongod_set }
}

sub start {
    my ($self) = @_;
    for my $server ( $self->list_mongod_set ) {
        $self->_logger->info("starting $server");
        $server->start;
        $self->_logger->info("$server is up on port " . $server->port);
    }
}

sub stop {
    my ($self) = @_;
    for my $server ( $self->list_mongod_set ) {
        $self->_logger->info("stopping $server");
        $server->stop;
    }
}

sub as_uri {
    my ($self) = @_;
    my $uri = "mongodb://" . join(",", map { $_->hostname . ":" . $_->port } $self->list_mongod_set);
    return $uri;
}

sub DEMOLISH {
    my ($self) = @_;
    $self->stop;
}

1;
