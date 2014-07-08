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

package MongoDBTest::Cluster;

use MongoDBTest::ServerSet;
use MongoDBTest::ReplicaSet;

use Moo;
use Types::Standard -types;
use namespace::clean;

has server_set => (
    is => 'lazy',
    isa => ConsumerOf['MongoDBTest::Role::ServerSet'],
    handles => [qw/start stop as_uri as_pairs get_server all_servers/]
);

my $anon_rs_number = 0;

sub _build_server_set {
    my ($self) = @_;

    my $class =  "MongoDBTest::" . ($self->is_replica ? "ReplicaSet" : "ServerSet");
    return $class->new(
        default_args => $self->default_args,
        default_version => $self->default_version,
        timeout => $self->timeout,
        auth_config => $self->auth_config,
        server_config_list => $self->config->{mongod},
        $self->is_replica ? ( set_name => $self->config->{setName} // "rs" . $anon_rs_number++ ) : (),
    );
}

with 'MongoDBTest::Role::Cluster';

1;
