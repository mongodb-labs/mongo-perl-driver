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

use MongoDB;

use CPAN::Meta::Requirements;
use JSON;
use Path::Tiny;
use POSIX qw/SIGTERM SIGKILL/;
use Proc::Guard;
use File::Spec;
use Safe::Isa;
use Sys::Hostname;
use Try::Tiny::Retry 0.004 ":all";
use Net::EmptyPort qw/empty_port wait_port/;
use Version::Next qw/next_version/;
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

has port_override => (
    is => 'lazy',
    isa => Num|Undef,
);

sub _build_port_override {
    my ($self) = @_;
    return $self->config->{port_override};
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
    is => 'lazy',
    isa => Str,
);

sub _build_timeout {
    my ($self) = @_;
    return $ENV{MONGOTIMEOUT} || 120;
}

has hostname => (
    is => 'lazy',
    isa => Str,
);

sub _build_hostname { hostname() }

has server_version => (
    is => 'rwp',
    isa => InstanceOf['version'],
);

has version_wanted => (
    is => 'lazy',
    isa => Str,
);

sub _build_version_wanted {
    my ($self) = @_;
    my $target = ($self->config->{version} // $self->default_version) || 0;
    return $target;
}

has version_constraint => (
    is => 'lazy',
    isa => InstanceOf['CPAN::Meta::Requirements'],
);

sub _build_version_constraint {
    my ($self) = @_;
    # abusing CMR for this
    my $cmr = CPAN::Meta::Requirements->new;
    my $target = $self->version_wanted;
    if ( $target =~ /^v?\d+\.\d+\.\d+$/ ) {
        $target =~ s/^v?(.*)/v$1/;
        $cmr->exact_version(mongo => $target)
    }
    elsif ( $target =~ /^v?\d+\.\d+$/ ) {
        $target =~ s/^v?(.*)/v$1/;
        $cmr->add_minimum(mongo => $target);
        $cmr->add_string_requirement(mongo => "< " . next_version($target));
    }
    else {
        # hope it's a valid version range specifier
        $cmr->add_string_requirement(mongo => $target);
    }
    return $cmr;
}

has executable => (
    is => 'lazy',
    isa => AbsFile,
    coerce => AbsFile->coercion,
);

sub _build_executable {
    my ($self) = @_;

    my $cmd = $self->command_name;
    my @paths = split /:/, $ENV{PATH};
    unshift @paths, split /:/, $ENV{MONGOPATH} if $ENV{MONGOPATH};

    for my $f ( grep { -x } map { path($_)->child($cmd) } @paths ) {
        my $v_check = qx/$f --version/;
        my ($found_version) = $v_check =~ /version (v?\d+\.\d+\.\d+)/;
        $self->_set_server_version(version->parse($found_version));
        if ( $self->version_wanted ) {
            if ( $self->version_constraint->accepts_module( mongo => $found_version ) ) {
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

sub _build_tempdir {
    my $self = shift;
    return Path::Tiny->tempdir(
        TEMPLATE => $self->name . "-XXXXXX",
        ($ENV{DATA_DIR} ? (DIR => $ENV{DATA_DIR}) : ()),
    );
}

has logfile => (
    is => 'lazy',
    isa => AbsPath,
    coerce => AbsPath->coercion,
);

sub _build_logfile {
    my ($self) = @_;
    if ( $ENV{MONGOLOGDIR} ) {
        return path($ENV{MONGOLOGDIR})->absolute->child( $self->name . ".log" );
    }
    return $self->tempdir->child("mongodb.log");
}

has auth_config => (
    is => 'ro',
    isa => Maybe[HashRef],
);

has did_auth_setup => (
    is => 'rwp',
    isa => Bool,
);

has ssl_config => (
    is => 'ro',
    isa => Maybe[HashRef],
);

has did_ssl_auth_setup => (
    is => 'rwp',
    isa => Bool,
);

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

has client => (
    is => 'lazy',
    isa => InstanceOf['MongoDB::MongoClient'],
    clearer => 1,
);

sub _build_client {
    my ($self) = @_;
    my @args = (
        host    => $self->as_uri_with_auth,
        dt_type => undef,
    );
    if ( my $ssl = $self->ssl_config ) {
        my $ssl_arg = {};
        $ssl_arg->{SSL_verifycn_scheme} = 'none';
        $ssl_arg->{SSL_ca_file} = $ssl->{certs}{ca}
          if $ssl->{certs}{ca};
        $ssl_arg->{SSL_verifycn_name} = $ssl->{servercn}
          if $ssl->{servercn};
        $ssl_arg->{SSL_hostname} = $ssl->{servercn}
          if $ssl->{servercn};
        if ( $self->did_ssl_auth_setup ) {
            push @args,
              (
                username       => $ssl->{username},
                auth_mechanism => 'MONGODB-X509',
              );
            $ssl_arg->{SSL_cert_file} = $ssl->{certs}{client};
        }
        push @args, ssl => $ssl_arg;
    }

    return MongoDB::MongoClient->new( @args );
}

# Methods

sub start {
    my ($self, $port) = @_;
    retry {
        if ( defined $self->port_override ) {
            $self->_set_port( $self->port_override );
        }
        else {
            defined $port ? $self->_set_port($port) : $self->_set_port(empty_port());
        }
        $self->_logger->debug( "Running "
              . $self->executable . " "
              . join( " ", $self->_command_args( $self->auth_config && !$self->did_auth_setup ) )
        );
        my $guard = proc_guard(
            sub {
                open STDOUT, ">", File::Spec->devnull unless $self->verbose;
                open STDERR, ">", File::Spec->devnull unless $self->verbose;
                open STDIN, "<", File::Spec->devnull;
                exec( $self->executable,
                    $self->_command_args( $self->auth_config && !$self->did_auth_setup ) )
            }
        );
        $self->_set_guard( $guard );
        $self->_logger->debug("Waiting for port " . $self->port);
        # XXX eventually refactor out so this can be done in parallel
        wait_port($self->port, $self->timeout)
            or die sprintf("Timed out waiting for %s on port %d after %d seconds\n", $self->name, $self->port, $self->timeout);

        # wait for the server to respond to ismaster
        retry {
            $self->_logger->debug(sprintf("Pinging %s (%s) with ismaster", $self->name, $self->as_uri));
            $self->clear_client;
            $self->client->get_database("admin")->run_command( [ ismaster => 1 ] );
        }
        delay_exp { 13, 1e5 }
        on_retry {
            warn $_;
        }
        catch {
            chomp;
            die "Host seems up, but ismaster is failing: $_"
        };

    }
    on_retry {
        warn $_;
        $self->clear_guard;
        $self->_logger->debug("Retrying server start for " . $self->name);
    }
    delay {
        return if $_[0] > 2;
    }
    catch { chomp; s/at \S+ line \d+//; die "Caught error:$_. Giving up!\n" };

    if ( $self->auth_config && !$self->did_auth_setup ) {
        my ( $user, $password ) = @{ $self->auth_config }{qw/user password/};
        $self->add_user( "admin", $user, $password, [ {role => 'root', db => 'admin' } ] );
        $self->_set_did_auth_setup(1);
        $self->_logger->debug("Restarting original server with --auth");
        $self->_local_restart;
    }

    if (   $self->ssl_config
        && $self->ssl_config->{username}
        && !$self->did_ssl_auth_setup )
    {
        $self->add_user( '$external', $self->ssl_config->{username},
            '', [ { role => 'readWrite', db => $self->{ssl_config}{db} || 'x509' } ] );
        $self->_set_did_ssl_auth_setup(1);
        $self->_logger->debug("Restarting original server with SSL");
        $self->_local_restart;
    }
    return 1;
}

sub _local_restart {
    my ($self) = @_;
    my $port = $self->port;
    # must be localhost for shutdown command
    my $uri = "mongodb://localhost:$port";
    eval {
        my $client = $self->get_direct_client($uri, connect_type => 'direct');
        $client->get_database("admin")->run_command( [ shutdown => 1 ] );
    };
    my $err = $@;
    # 'shutdown' closes the socket so we don't get a network error instead
    # of a response.  We can ignore that case.
    if ( ! $err->$_isa("MongoDB::NetworkError") ) {
        $self->_logger->debug("Error on shutdown for localhost:$port: $err");
    }
    $self->stop;
    $self->clear_client;
    $self->start;
}

sub stop {
    my ($self) = @_;
    if ( $self->has_guard ) {
        # will give 30 seconds for graceful shutdown
        eval {
            local $SIG{ALRM} = sub { die "alarm\n" };
            alarm 30;
            $self->guard->stop(SIGTERM);
            alarm 0;
        };
        if ($@) {
            die unless $@ eq "alarm\n"; # propagate unexpected errors
            # SIGTERM timed out so force it to stop
            $self->guard->stop(SIGKILL);
        }
        $self->clear_guard;
    }
    $self->clear_port;
    $self->clear_client;
    $self->_logger->debug("cleared guard and client for " . $self->name);
}

sub get_direct_client {
    my ($self, $uri, @args) = @_;
    $uri //= $self->as_uri_with_auth;
    my $config = { host => $uri, dt_type => undef, @args };
    if ( my $ssl = $self->ssl_config ) {
        my $ssl_arg = {};
        $ssl_arg->{SSL_verifycn_scheme} = 'none';
        $ssl_arg->{SSL_ca_file}         = $ssl->{certs}{ca}
          if $ssl->{certs}{ca};
        $ssl_arg->{SSL_verifycn_name} = $ssl->{servercn}
          if $ssl->{servercn};
        $ssl_arg->{SSL_hostname} = $ssl->{servercn}
          if $ssl->{servercn};
        if ( $self->did_ssl_auth_setup ) {
            $config->{username}       = $ssl->{username};
            $config->{auth_mechanism} = 'MONGODB-X509';
            $ssl_arg->{SSL_cert_file} = $ssl->{certs}{client};
        }
        $config->{ssl} = $ssl_arg;
    }
    return MongoDB::MongoClient->new($config);
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

sub as_uri_with_auth {
    my ($self) = @_;
    my $uri = $self->as_uri;
    if ( $self->auth_config && $self->did_auth_setup ) {
        my ( $u, $p ) = @{ $self->auth_config }{qw/user password/};
        $uri =~ s{mongodb://}{mongodb://$u:$p\@};
    }
    return $uri;
}

sub _filtered_default_args {
    my ($self) = @_;
    my @filtered;
    for my $s ( split ' ', $self->default_args ) {
        # let verbose logging override default
        next if $s =~ /^-v/ && $self->log_verbose;
        # 3.6+ errors on httpinterface
        next if $s =~ /httpinterface/ && $self->server_version > v3.4.0;

        push @filtered, $s;
    }
    return @filtered;
}

sub _command_args {
    my ($self, $skip_replset ) = @_;
    my @args;
    if ( $self->server_version < v2.6.0 && $self->command_name eq 'mongos' ) {
        push @args, "-v"; # 2.4 mongos has problems with too much verbosity
    }
    else {
        push @args, ( $self->log_verbose ? "-vvv" : "-v" );
    }
    push @args, $self->_filtered_default_args;
    push @args, split ' ', $self->config->{args} if exists $self->config->{args};
    push @args, split ' ', $self->command_args;
    push @args, '--port', $self->port, '--logpath', $self->logfile, '--logappend';
    if ($self->did_auth_setup) {
        push @args, '--auth';
    }
    if (my $ssl = $self->ssl_config) {
        push @args, '--sslMode', $ssl->{mode} || 'allowSSL'
            if $self->server_version >= v3.0.0;
        push @args, '--sslPEMKeyFile', $ssl->{certs}{server};
        push @args, '--sslCAFile', $ssl->{certs}{ca};
        push @args, '--sslCRLFile', $ssl->{certs}{crl}
            if $ssl->{certs}{crl};
        if (! $self->did_ssl_auth_setup) {
            push @args,
                $self->server_version >= v3.0.0
                ? '--sslAllowConnectionsWithoutCertificates'
                : '--sslWeakCertificateValidation';
            push @args, '--sslAllowInvalidCertificates';
        }
    }
    if ( $self->server_version >= v2.4.0 ) {
        push @args, qw/--setParameter enableTestCommands=1/;
        if ( ($self->server_version < v2.5.0) ) {
            push @args, qw/--setParameter textSearchEnabled=true/;
        }
    }

    if ($skip_replset) {
        my @new_args;
        for ( my $i=0; $i < @args; $i++ ) {
            if ($args[$i] eq '--replSet' || $args[$i] eq '--keyFile') {
                $i++;
                next;
            }
            push @new_args, $args[$i];
        }
        @args = @new_args;
    }

    return @args;
}

sub add_user {
    my ($self, $db, $user, $password, $roles) = @_;
    return unless $user;
    $self->_logger->debug("Adding authorized user");
    my $doc = Tie::IxHash->new(
        ( $password ? ( pwd => $password ) : () ),
        roles => $roles,
    );
    if ( $self->server_version >= v2.6.0 ) {
        $doc->Unshift(createUser => $user);
        $self->client->get_database($db)->run_command( $doc );
    }
    else {
        $doc->Unshift(user => $user);
        $self->client->get_database($db)->get_collection("system.users")->save( $doc );
    }
    return;
}

sub grep_log {
    my ($self, $re) = @_;
    return grep { /$re/ } $self->logfile->lines;
}

sub DEMOLISH {
    my ($self) = @_;
    $self->stop if $self->is_alive;
}

with 'MongoDBTest::Role::Verbosity';

1;
