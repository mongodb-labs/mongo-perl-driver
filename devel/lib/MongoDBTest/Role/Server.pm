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

package MongoDBTest::Role::Server;

use Path::Tiny;
use Proc::Guard;
use Sys::Hostname;
use Net::EmptyPort qw/empty_port wait_port/;
use version;

use Moo::Role;
use Types::Standard -types;
use Types::Path::Tiny qw/AbsFile AbsDir AbsPath/;
use namespace::clean;

# To be satisfied by consumer

requires '_build_command_name';
requires '_build_command_args';
requires '_logger';

has command_args => (
    is => 'lazy',
    isa => Str,
);

has command_name => (
    is => 'lazy',
    isa => Str,
);

# Required

has config => (
    is => 'ro',
    isa => HashRef,
    required => 1,
);

# Default or Lazy

has name => (
    is => 'lazy',
    isa => Str,
);

sub _build_name {
    my ($self) = @_;
    return $self->config->{name};
}

has default_args => (
    is => 'ro',
    isa => Str,
    default => '',
);

has default_version => (
    is => 'ro',
    isa => Str,
    default => '',
);

has timeout => (
    is => 'ro',
    isa => Str,
    default => 60,
);

has hostname => (
    is => 'lazy',
    isa => Str,
);

sub _build_hostname { hostname() }

has version => (
    is => 'lazy',
    isa => InstanceOf['version'],
);

sub _build_version {
    my ($self) = @_;
    return version->parse( $self->config->{version} // $self->default_version );
}

has executable => (
    is => 'lazy',
    isa => AbsFile,
    coerce => AbsFile->coercion,
);

sub _build_executable {
    my ($self) = @_;

    my $cmd = $self->command_name;
    my $want_version = $self->version;
    my @paths = split /:/, $ENV{PATH};
    unshift @paths, split /:/, $ENV{MONGOPATH} if $ENV{MONGOPATH};

    for my $f ( grep { -x } map { path($_)->child($cmd) } @paths ) {
        if ( $want_version ) {
            my $v_check = qx/$f --version/;
            my ($found_version) = $v_check =~ /version (v?\d+\.\d+\.\d+)/;
            if ( $found_version == $want_version ) {
                $self->_logger->debug("$f is $found_version");
                return $f;
            }
        }
        else {
            return $f;
        }
    }

    die "Can't find suitable $cmd in MONGOPATH or PATH\n";
}

has tempdir => (
    is => 'lazy',
    isa => AbsDir,
    coerce => AbsDir->coercion,
);

sub _build_tempdir { Path::Tiny->tempdir }

has logfile => (
    is => 'lazy',
    isa => AbsPath,
    coerce => AbsPath->coercion,
);

sub _build_logfile {
    my ($self) = @_;
    return $self->tempdir->child("mongodb.log");
}

# Semi-private

has port => (
    is => 'rwp',
    isa => Num,
    clearer => 1,
);

has guard => (
    is => 'rwp',
    isa => InstanceOf['Proc::Guard'],
    clearer => 1,
    predicate => 1,
);

# Methods

sub start {
    my ($self) = @_;
    $self->_set_port(empty_port());
    $self->_logger->debug("Running " . $self->executable . " " . join(" ", $self->_command_args));
    my $guard = proc_guard(
        sub {
            close STDOUT;
            close STDERR;
            close STDIN;
            exec( $self->executable, $self->_command_args );
        }
    );
    $self->_set_guard( $guard );
    $self->_logger->debug("Waiting for port " . $self->port);
    # XXX eventually refactor out so this can be done in parallel
    wait_port($self->port, $self->timeout);
    return 1;
}

sub stop {
    my ($self) = @_;
    $self->clear_guard;
    $self->clear_port;
}

sub is_alive {
    my ($self) = @_;
    return unless $self->has_guard;
}

sub as_host_port {
    my ($self) = @_;
    return $self->hostname . ":" . $self->port;
}

sub as_uri {
    my ($self) = @_;
    return "mongodb://" . $self->as_host_port;
}

sub _command_args {
    my ($self) = @_;
    my @args = split ' ', $self->default_args;
    push @args, split ' ', $self->config->{args} if exists $self->config->{args};
    push @args, split ' ', $self->command_args;
    push @args, '--port', $self->port, '--logpath', $self->logfile;

    return @args;
}

sub DEMOLISH {
    my ($self) = @_;
    $self->stop;
}

1;
