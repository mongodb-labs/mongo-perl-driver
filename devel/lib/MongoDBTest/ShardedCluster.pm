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

package MongoDBTest::ShardedCluster;

# ABSTRACT: a test sharded cluster

use Moose;
use Moose::Util::TypeConstraints;
use MongoDB;
use Carp 'carp', 'croak';
use File::Spec;
use File::Path; 

has mongo_path => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => '~/10gen/mongo/'
);

has dbpath => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => '/data/db/sharding'
);

has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017 
);

has chunksize => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 64 
);

has shardns => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);

has shardkey => (
    is       => 'ro',
    isa      => 'HashRef',
    required => 1
);

has n_configs => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 1
);

has n_shards => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 3
);

has n_mongos => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 1
);

has client => (
    is       => 'rw',
    isa      => 'MongoDB::MongoClient'
);

has _config_port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 20000
);

has _shard_port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 30000
);

has _nodes => (
    is       => 'rw',
    isa      => 'HashRef',
    default  => sub { {} }
);


sub BUILD {
    my ($self, $opts) = @_;

    my $mongod = File::Spec->catfile($self->mongo_path, 'mongod');
    croak "can't find mongod" unless -e $mongod;
    
    my $mongos = File::Spec->catfile($self->mongo_path, 'mongos');
    croak "can't find mongos" unless -e $mongos;

    if ($self->n_configs != 1 && $self->n_configs != 3) {
        croak "number of config servers must be one or three; " .
                    "you specified " . $self->configs;
    }

    # wipe data if already there
    if (-e $self->dbpath) {
        File::Path->remove_tree($self->dbpath);
    }
    File::Path->make_path($self->dbpath);

    # set up config servers
    my @config_hosts;
    foreach (0..($self->n_configs - 1)) {
        my $path = File::Spec->catfile($self->dbpath, "config_$_");
        mkdir $path;

        my $port = $self->_config_port + $_;
        my $host = "localhost:$port";
        push @config_hosts, $host;

        my @command = ($mongod, '--configsvr');
        push @command, ('--port', $port); 
        push @command, ('--dbpath', $path);
        push @command, ('--bind_ip', '127.0.0.1');

        my $pid = fork;
        if (!$pid) {
            # child process runs mongod
            exec join(' ', @command);
        }
        elsif ($pid > 0) {
            $self->_nodes->{$host} = $pid;
        }
    }

    # set up shard servers
    my @shard_hosts;
    foreach (0..($self->n_shards - 1)) {
        my $path = File::Spec->catfile($self->dbpath, "shard_$_");
        mkdir $path;

        my $port = $self->_shard_port + $_;
        my $host = "localhost:$port";
        push @shard_hosts, $host;

        my @command = ($mongod, '--shardsvr');
        push @command, ('--port', $port);
        push @command, ('--dbpath', $path);
        push @command, ('--bind_ip', '127.0.0.1');

        my $pid = fork;
        if (!$pid) {
            # child process runs mongod
            exec join(' ', @command);
        }
        elsif ($pid > 0) {
            $self->_nodes->{$host} = $pid;
        }
    }

    # wait, just to be safe
    sleep 2;

    # set chunksize
    foreach (@config_hosts) {
        if ($_ =~ /(\w+):(\d+)/) {
            my $client = MongoDB::MongoClient->new(
                host => "mongodb://localhost:$2"
            );
            my $db = $client->get_database('config');
            my $coll = $db->get_collection('settings');
            $coll->save({_id => 'chunksize', 'value' => $self->chunksize});
            undef $client;
        }
    }

    # wait, just to be safe
    sleep 2;

    # start mongos
    foreach (0..($self->n_mongos - 1)) {
        my @command = ($mongos, '--port', $self->port);
        push @command, ('--configdb', join(',', @config_hosts));
        push @command, ('--bind_ip', '127.0.0.1');

        my $pid = fork;
        if (!$pid) {
            # child process runs mongod
            exec join(' ', @command);
        }
        elsif ($pid > 0) {
            $self->_nodes->{'localhost:' . $self->port} = $pid;
        }
    }

    # wait, just to be safe
    sleep 2;

    my $client = MongoDB::MongoClient->new(
        host => 'mongodb://localhost:' . $self->port,
        find_master => 1
    );
    $self->client($client);
    my $admin = $client->get_database('admin');

    # add shards
    foreach (@shard_hosts) {
        $admin->run_command({addShard => $_});
    }

    # enable sharding
    my ($shard_db, $shard_coll) = ($self->shardns =~ /(\w+)\.(\S+)/);
    $admin->run_command({enableSharding => $shard_db});
    $admin->run_command({shardCollection => $self->shardns, key => $self->shardkey});

    # wait, just to be safe
    sleep 2;
}


sub shutdown {
    my ($self) = @_;
    
    my %nodes = %{$self->_nodes};
    my $to_die = keys %nodes;
    my @pids = @nodes{keys %nodes};

    my $dead = kill 'SIGTERM', @pids;
    if ($to_die != $dead) {
        warn "$dead out of $to_die RS members shutdown properly";
    }
}


1;


