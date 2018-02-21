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

package MongoDBTest::ShardedCluster;

use MongoDB;
use MongoDBTest::Deployment;
use MongoDBTest::ServerSet;
use MongoDBTest::Mongod;

use JSON;
use Moo;
use Try::Tiny::Retry qw/:all/;
use Types::Standard -types;
use Type::Utils qw/declare as/;
use namespace::clean;


my $SERVERSET = declare as ConsumerOf['MongoDBTest::Role::ServerSet'];

has with_CSRS => (
    is => 'lazy',
    isa => Bool,
);

sub _build_with_CSRS {
    my $self=shift;
    my $temp_server = MongoDBTest::Mongod->new(
        config => { name => "temp" },
        default_version => $self->config->{default_version} // 0,
        default_fcv => $self->config->{default_fcv} // "",
    );
    return -x $temp_server->executable && $temp_server->server_version >= v3.2.0;
}

has config_servers => (
    is => 'lazy',
    isa => $SERVERSET,
);

sub _build_config_servers {
    my ($self) = @_;
    my $class = $self->with_CSRS ? "MongoDBTest::ReplicaSet" : "MongoDBTest::ServerSet";
    return $class->new(
        # don't pass default args from config file
        default_args => "--configsvr --bind_ip 0.0.0.0",
        default_version => $self->default_version,
        default_fcv => $self->default_fcv,
        server_config_list => $self->config->{mongoc},
        ( $self->with_CSRS ? ( set_name => "configReplSet" ) : () ),
        verbose => $self->verbose,
        log_verbose => $self->log_verbose,
    );
}

has routers => (
    is => 'lazy',
    isa => $SERVERSET,
    handles => [ qw/as_uri as_pairs/ ],
);

sub _build_routers {
    my ($self) = @_;
    my $config_names = $self->config_servers->as_pairs;
    $config_names = "configReplSet/$config_names" if $self->with_CSRS;
    return MongoDBTest::ServerSet->new(
        # don't pass default args from config file
        default_args => "--configdb $config_names --bind_ip 0.0.0.0",
        default_version => $self->default_version,
        default_fcv => $self->default_fcv,
        server_config_list => $self->config->{mongos},
        server_type => 'mongos',
        verbose => $self->verbose,
        log_verbose => $self->log_verbose,
    );
}

has shard_sets => (
    is => 'lazy',
    isa => HashRef[ConsumerOf['MongoDBTest::Role::Deployment']],
);

sub _build_shard_sets {
    my ($self) = @_;

    my $set = {};

    for my $shard ( @{ $self->config->{shards} } ) {
        my $name = $shard->{name};
        # args are additive, version is not
        $shard->{default_args} = "--shardsvr " . $self->default_args . ($shard->{default_args} // "");
        $shard->{default_version} //= $self->default_version,
        $shard->{default_fcv} //= $self->default_fcv,

        $set->{$name} = MongoDBTest::Deployment->new( config => $shard );
    }

    return $set;
}

sub start {
    my ($self) = @_;

    $self->_logger->debug("starting config servers");
    $self->config_servers->start;
    $self->_logger->debug("starting mongos servers");
    $self->routers->start;

    my $uri = $self->routers->as_uri;
    $self->_logger->debug("connecting to mongos at $uri");
    my $client =
        retry {
            my $client = $self->get_client;
            $client->db("admin")->run_command({ismaster => 1});
            $client
        }
        on_retry { $self->_logger->debug($_) }
        delay_exp { 15, 1e4 }
        catch { chomp; die "$_. Giving up!\n" };

    my $admin_db = $client->get_database("admin");

    # XXX later maybe do shard start in parallel with other start and loop later to add
    for my $k ( sort keys %{ $self->shard_sets } ) {
        my $shard = $self->shard_sets->{$k};

        $self->_logger->debug("starting shard $k");
        $shard->start;

        $self->_logger->debug("adding shard $k");
        my $pairs = $shard->as_pairs;
        if ( $shard->is_replica ) {
            $pairs = join("/", $shard->server_set->set_name, $pairs);
        }
        $admin_db->run_command([addShard => $pairs, name => $k]);
    }

    return;
}

sub stop {
    my ($self) = @_;
    $self->routers->stop;
    $self->config_servers->stop;
    for my $shard ( $self->all_shards ) {
        $shard->stop;
    }
    return;
}

sub all_shards {
    my ($self) = @_;
    return values %{ $self->shard_sets };
}

sub all_servers {
    my ($self) = @_;
    my @servers = map { $_->all_servers } $self->all_shards;
    push @servers, $self->routers->all_servers, $self->config_servers->all_servers;
    return @servers;
}

sub get_server {
    my ($self, $name) = @_;
    for my $server ( $self->all_servers ) {
        return $server if $server->name eq $name;
    }
    return;
}

sub get_client {
    my ($self) = @_;
    my $config = { host => $self->as_uri, dt_type => undef };
    if ( my $ssl = $self->ssl_config ) {
        my $ssl_arg = {};
        $ssl_arg->{SSL_verifycn_scheme} = 'none';
        $ssl_arg->{SSL_ca_file}         = $ssl->{certs}{ca}
          if $ssl->{certs}{ca};
        $ssl_arg->{SSL_verifycn_name} = $ssl->{servercn}
          if $ssl->{servercn};
        $ssl_arg->{SSL_hostname} = $ssl->{servercn}
          if $ssl->{servercn};
        if ($ssl->{username}) {
            $config->{username}       = $ssl->{username};
            $config->{auth_mechanism} = 'MONGODB-X509';
            $ssl_arg->{SSL_cert_file} = $ssl->{certs}{client};
        }
        $config->{ssl} = $ssl_arg;
    }
    $self->_logger->debug("connecting to server with: " . to_json($config));

    return MongoDB::MongoClient->new($config);
}

with 'MooseX::Role::Logger', 'MongoDBTest::Role::Deployment';

1;
