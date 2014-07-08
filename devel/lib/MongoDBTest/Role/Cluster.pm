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

package MongoDBTest::Role::Cluster;

use Moo::Role;
use Types::Standard -types;
use namespace::clean;

requires 'start';
requires 'stop';
requires 'as_uri';
requires 'as_pairs';
requires 'get_server';
requires 'all_servers';

has config => (
    is => 'ro',
    isa => HashRef,
    required => 1,
);

has default_args => (
    is => 'lazy',
    isa => Str,
);

sub _build_default_args {
    my ($self) = @_;
    return $self->config->{default_args} // '';
}

has default_version => (
    is => 'lazy',
    isa => Str,
);

has timeout => (
    is => 'lazy',
    isa => Maybe[Num],
);

sub _build_timeout {
    my ($self) = @_;
    return $self->config->{timeout};
}

sub _build_default_version {
    my ($self) = @_;
    return $self->config->{default_version} // '';
}

has auth_config => (
    is => 'lazy',
    isa => Maybe[HashRef],
);

sub _build_auth_config {
    my ($self) = @_;
    return $self->config->{auth};
}

has type => (
    is => 'lazy',
    isa => Enum[qw/single replica sharded/],
);

sub _build_type {
    my ($self) = @_;
    $self->config->{type};
}

sub is_replica {
    my ($self) = @_;
    return $self->type eq 'replica';
}

sub DEMOLISH {
    my ($self) = @_;
    $self->stop;
}

1;
