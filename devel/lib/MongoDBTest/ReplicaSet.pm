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

package MongoDBTest::ReplicaSet;

use MongoDB;

use JSON;
use Moo;
use Types::Standard -types;
use namespace::clean;

with 'MooseX::Role::Logger', 'MongoDBTest::Role::ServerSet';

has set_name => (
    is => 'ro',
    isa => Str,
    required => 1,
);

has client => (
    is => 'lazy',
    isa => InstanceOf['MongoDB::MongoClient'],
);

sub _build_client {
    my ($self) = @_;
    return MongoDB::MongoClient->new( host => $self->as_uri, find_master => 1, dt_type => undef );
}

after 'start' => sub {
    my ($self) = @_;
    $self->rs_initiate;
};

sub BUILD {
    my ($self) = @_;
    $self->_set_default_args( $self->default_args . " --replSet " . $self->set_name );
}

sub rs_initiate {
    my ($self) = @_;
    my ($first) = $self->all_servers;

    # XXX eventually may have to figure out adding additional parameters
    my $members = [
        map {;
            {
                host => $_->as_host_port,
                %{$_->config->{rs_config} // {}}
            }
        }
        sort { $a->name cmp $b->name } $self->all_servers
    ];

    for my $i (0 .. $#$members) {
        $members->[$i]{_id} = $i;
    }

    my $rs_config = {
        _id => $self->set_name,
        members => $members,
    };

    $self->_logger->debug("configuring replica set with: " . to_json($rs_config));

    # not $self->client because this needs to be a direct connection
    my $client = MongoDB::MongoClient->new( host => $first->as_uri, dt_type => undef );
    $client->get_database("admin")->_try_run_command({replSetInitiate => $rs_config});

    $self->_logger->debug("waiting for primary");
    my $c = 1;
    $self->client->get_master;

    return;
}

sub wait_for_all_hosts {
    my ($self) = @_;
    my $client = eval {$self->client};
    die "Couldn't get client: $@" if $@;
    my $admin = $client->get_database("admin");
    my $c = 1;
    while (1) {
        if ( my $status = eval { $admin->_try_run_command({replSetGetStatus => 1}) } ) {
            my @member_states = map { $_->{state} } @{ $status->{members} };
            $self->_logger->debug("host states: @member_states");
            last if @member_states == grep { $_ == 1 || $_ == 2 } @member_states; # all primary or secondary
        }
        $self->_logger->debug("waiting for all hosts to be online")
            if $c++ % 5 == 0;
        die "never got all hosts up\n" if $c > 120; # XXX hard coded
    }
    continue { sleep 1 }

    return;
}

sub stepdown_primary {
    my ($self, $timeout_secs) = @_;
    # eval because command causes primary to close connection, causing network error
    eval {
        $self->client->get_database("admin")->_try_run_command( { replSetStepDown => 5 } );
    };
    return;
}

1;
