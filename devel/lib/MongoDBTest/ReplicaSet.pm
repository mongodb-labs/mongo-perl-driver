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
use MIME::Base64 qw/encode_base64/;
use Path::Tiny;
use Try::Tiny::Retry qw/:all/;
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
    return $self->get_client;
}

has 'keyfile' => (
    is => 'lazy',
    isa => InstanceOf['Path::Tiny'],
);

sub _build_keyfile {
    my ($self) = @_;
    my $file = Path::Tiny->tempfile;
    $file->chmod("0600");
    $file->append(encode_base64(join("", map { ["a" .. "z"]->[int(rand(26))] } 1 .. 100)));
    return $file;
}

after 'start' => sub {
    my ($self) = @_;
    $self->rs_initiate;
    $self->wait_for_primary;
};

# override to only set up auth on the first server
sub _build__servers {
    my ($self) = @_;
    my $set = {};
    my $did_first;
    for my $server ( sort { $a->{name} cmp $b->{name} } @{ $self->server_config_list } )
    {
        my $class = "MongoDBTest::" . ucfirst( $self->server_type );
        $set->{ $server->{name} } = $class->new(
            config          => $server,
            default_args    => $self->default_args,
            default_version => $self->default_version,
            ( $did_first ? () : ( auth_config => $self->auth_config ) ),
            ssl_config => $self->ssl_config,
            ( $self->timeout ? ( timeout => $self->timeout ) : () ),
            verbose     => $self->verbose,
            log_verbose => $self->log_verbose,
        );
        $did_first++;
    }
    return $set;
}

sub BUILD {
    my ($self) = @_;
    my $new_args = $self->default_args . " --replSet " . $self->set_name;
    $new_args .= " --keyFile " . $self->keyfile if $self->auth_config;
    $self->_set_default_args( $new_args );
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

    $self->_logger->debug(
        "configuring replica set on @{[$first->name]} with: " . to_json($rs_config) );

    # not $self->client because this needs to be a direct connection, i.e. one
    # seed and no replicaSet URI option

    my $client = $first->get_direct_client;

    $client->get_database("admin")->run_command({ismaster => 1});

    $client->get_database("admin")->run_command({replSetInitiate => $rs_config});

    $self->wait_for_primary;

    return;
}

sub wait_for_all_hosts {
    my ($self) = @_;
    my ($first) = $self->all_servers;
    retry {
        my $client = $first->get_direct_client;
        my $admin = $client->get_database("admin");
        if ( my $status = eval { $admin->run_command({replSetGetStatus => 1}) } ) {
            my @member_states = map { $_->{state} } @{ $status->{members} };
            $self->_logger->debug("host states: @member_states");
            die "Hosts not all PRIMARY or SECONDARY or ARBITER\n"
              unless @member_states == grep { $_ == 1 || $_ == 2 || $_ == 7 } @member_states;
        }
        else {
            die "Can't get replica set status";
        }
    }
    delay {
        return if $_[0] >= 180;
        sleep 1;
    }
    catch { chomp; die "$_. Giving up!" };

    return;
}

sub wait_for_primary {
    my ($self)  = @_;
    my ($first) = $self->all_servers;
    my $uri = $first->as_uri_with_auth;
    $self->_logger->debug("Waiting from primary on URI: $uri");
    retry {
        my $client = $first->get_direct_client;
        my $admin  = $client->get_database("admin");
        if ( my $status = eval { $admin->run_command( { replSetGetStatus => 1 } ) } ) {
            my @member_states = map { $_->{state} } @{ $status->{members} };
            $self->_logger->debug("host states: @member_states");
            die "No PRIMARY\n"
              unless grep { $_ == 1 } @member_states;
        }
        else {
            die "Can't get replica set status";
        }
    }
    delay {
        return if $_[0] >= 180;
        sleep 1;
    }
    catch { chomp; die "$_. Giving up!" };

    return;
}

sub stepdown_primary {
    my ($self, $timeout_secs) = @_;
    # eval because command causes primary to close connection, causing network error
    eval {
        $self->client->get_database("admin")->run_command( { replSetStepDown => 5 } );
    };
    return;
}

around 'as_uri' => sub {
    my $orig = shift;
    my $self = shift;
    my $uri = $self->$orig;
    my $set = $self->set_name;
    $uri =~ s{/?$}{/};
    $uri .= "?replicaSet=$set";
    return $uri;
};

1;
