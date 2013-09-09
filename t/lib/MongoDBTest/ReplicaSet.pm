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

package MongoDBTest::ReplicaSet;

# ABSTRACT: a test replica set 

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
    default  => '/data/db/replset'
);

has logpath => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);

has logappend => (
    is       => 'ro',
    isa      => 'Str',
    default  => 0,
    required => 1
);

has set_size => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 3
);

has oplog_size => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 100
);

has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017 
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1
);

has client => (
    is       => 'rw',
    isa      => 'MongoDB::MongoClient'
);

has priorities => (
    is       => 'ro',
    isa      => 'ArrayRef',
    required => 0
);

has _nodes => (
    is       => 'rw',
    isa      => 'HashRef',
    default  => sub { {} }
);

has _commands => (
    is       => 'rw',
    isa      => 'HashRef',
    default  => sub { {} }
);


sub BUILD {
    my ($self, $opts) = @_;

    my $mongod = File::Spec->catfile($self->mongo_path, 'mongod');
    Carp::croak "can't find mongod" unless -e $mongod;

    Carp::croak "logpath does not exist" unless -e $self->logpath;

    my @nodes;
    foreach (0..($self->set_size - 1)) {
        my $dbpath = File::Spec->catfile($self->dbpath, "rs_$_");

        # wipe data if already there
        if (-e $dbpath) {
            File::Path->remove_tree($dbpath);
        }
        File::Path->make_path($dbpath);

        my $port = $self->port + $_;
        my $host = "localhost:$port";
        push @nodes, $host;

        my $seed = $self->name . '/' . join(',', keys %{$self->_nodes});

        my $log = File::Spec->catfile($self->logpath, "rs_$_.log");
        if (!$self->logappend && -e $log) {
            unlink $log or warn "could not unlink $log";
        }

        my @command = ($mongod);
        push @command, ('--port', $port);
        push @command, ('--dbpath', $dbpath);
        push @command, ('--replSet', $seed);
        push @command, '--rest';
        push @command, ('--bind_ip', '127.0.0.1');
        push @command, ('--oplogSize', $self->oplog_size);
        push @command, ('--logpath', $log);

        # remember command so we can bring nodes back up
        $self->_commands->{$host} = \@command;
    }

    $self->nodes_up(@nodes);

    # build rs config document
    my $config = {_id => $self->name, members => []};
    foreach my $i (0 .. $#nodes) {
        my $member = {_id => $i, host => $nodes[$i]};
        $config->{'members'}->[$i] = $member;
        if ($self->priorities) {
            $config->{'members'}->[$i]->{'priority'} = $self->priorities->[$i];
        }
    }

    # wait for mongod's to start
    sleep 5;

    my $client = MongoDB::MongoClient->new(host => $nodes[$#nodes]);
    my $admin = $client->get_database('admin');
    $admin->run_command({replSetInitiate => $config});

    # wait for replica set initialization 
    foreach (1..60) {
        my $status = $admin->run_command({replSetGetStatus => 1});
        if (!ref($status)) {
            sleep 1;
        }
        elsif ($status->{'members'}) {
            my $is_ready = 1;
            foreach (@{$status->{'members'}}) {
                if ($_->{'state'} != 1 && $_->{'state'} != 2) {
                    $is_ready = 0;
                }
            }
            if ($is_ready) {
                # store a connection to an rs member
                $self->client(MongoDB::MongoClient->new(
                    host => 'mongodb://' . $nodes[0],
                    port => $self->port,
                    find_master => 1
                ));
                return;
            }
            sleep 1;
        }
    }

    die 'unable to create replica set';
}


sub shutdown {
    my ($self) = @_;
    $self->nodes_down(keys %{$self->_nodes});
    $self->_nodes({});
}


sub nodes_up {
    my ($self, @up) = @_;

    my %commands = %{$self->_commands};

    my @up_clean = map {
        if ($_ =~ /mongodb:\/\/(.*)/) {
            $1;
        }
        else {
            $_;
        }
    } @up;

    foreach (@up_clean) {
        # fork and run the mongod in a child process
        my $pid = fork;
        if (!$pid) {
            # child process runs mongod
            exec join(' ', @{$commands{$_}});
        }
        elsif ($pid > 0) {
            $self->_nodes->{$_} = $pid;
        }
    }
}


sub nodes_down {
    my ($self, @down) = @_;
    
    my $to_die = @down;
    my %nodes = %{$self->_nodes};

    my @down_clean = map {
        if ($_ =~ /mongodb:\/\/(.*)/) {
            $1;
        }
        else {
            $_;
        }
    } @down;
    my @pids = @nodes{@down_clean};

    my $dead = kill 'SIGTERM', @pids;
    if ($to_die != $dead) {
        warn "$dead out of $to_die RS members shutdown properly";
    }
}


sub add_tags {
    my ($self, @tags) = @_;

    my $client = $self->client;
    my $replcoll = $client->get_database('local')->get_collection('system.replset');
    my $rsconf = $replcoll->find_one();

    ($rsconf->{'version'})++;
    foreach my $i (0..$#tags) {
        $rsconf->{'members'}->[$i]{'tags'} = $tags[$i];
    }

    # reconfig will cause connection to be reset,
    # and throw a connection error
    eval {
        $client->get_database('admin')->run_command({'replSetReconfig' => $rsconf});
    };
    if ($@ !~ /can't get db response/) {
        die $@;
    }
}


1;

