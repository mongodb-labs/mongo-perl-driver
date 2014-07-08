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
use MongoDBTest::Cluster;
use MongoDBTest::ServerSet;

use Moo;
use Try::Tiny::Retry qw/:all/;
use Types::Standard -types;
use Type::Utils -all;
use namespace::clean;


my $SERVERSET = declare as ConsumerOf['MongoDBTest::Role::ServerSet'];

has config_servers => (
    is => 'lazy',
    isa => $SERVERSET,
);

sub _build_config_servers {
    my ($self) = @_;
    return MongoDBTest::ServerSet->new(
        default_args => "--configsvr " . $self->default_args,
        default_version => $self->default_version,
        server_config_list => $self->config->{mongoc},
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
    return MongoDBTest::ServerSet->new(
        default_args => "--configdb $config_names ", # don't pass default args
        default_version => $self->default_version,
        server_config_list => $self->config->{mongos},
        server_type => 'mongos',
    );
}

has shard_sets => (
    is => 'lazy',
    isa => HashRef[ConsumerOf['MongoDBTest::Role::Cluster']],
);

sub _build_shard_sets {
    my ($self) = @_;

    my $set = {};

    for my $shard ( @{ $self->config->{shards} } ) {
        my $name = $shard->{name};
        # args are additive, version is not
        $shard->{default_args} = $self->default_args . ($shard->{default_args} // "");
        $shard->{default_version} //= $self->default_version,

        $set->{$name} = MongoDBTest::Cluster->new( config => $shard );
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
        retry { MongoDB::MongoClient->new( host => $uri ) }
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

with 'MooseX::Role::Logger', 'MongoDBTest::Role::Cluster';

1;
