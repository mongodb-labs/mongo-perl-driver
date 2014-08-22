#
#  Copyright 2014 MongoDB, Inc.
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

package MongoDB::_Types;

# MongoDB Moose type definitions

use version;
our $VERSION = 'v0.704.4.1';

use Moose::Util::TypeConstraints;

sub connection_uri_re {
    return qr{
            mongodb://
            (?: ([^:]*) : ([^@]*) @ )? # [username:password@]
            ([^/]*) # host1[:port1][,host2[:port2],...[,hostN[:portN]]]
            (?:
               / ([^?]*) # /[database]
                (?: [?] (.*) )? # [?options]
            )?
    }x;
}

my $uri_re = MongoDB::_Types::connection_uri_re();

enum 'ClusterType',
  [qw/Single ReplicaSetNoPrimary ReplicaSetWithPrimary Sharded Unknown/];

enum 'ServerType',
  [
    qw/Standalone Mongos PossiblePrimary RSPrimary RSSecondary RSArbiter RSOther RSGhost Unknown/
  ];

enum 'ConnectType',
  [
    qw/replicaSet direct none/
  ];

class_type 'IxHash'            => { class => 'Tie::IxHash' };
class_type 'MongoDBCollection' => { class => 'MongoDB::Collection' };
class_type 'MongoDBDatabase'   => { class => 'MongoDB::Database' };

subtype ArrayOfHashRef => as 'ArrayRef[HashRef]';

subtype DBRefColl      => as 'Str';
subtype DBRefDB        => as 'Str';
subtype SASLMech       => as 'Str', where { /^GSSAPI|PLAIN$/ };
subtype
  ConnectionStr => as 'Str',
  where { $_ =~ /^$uri_re$/ },
  message { "Could not parse URI '$_'" };

# XXX loose address validation for now.  Host part should really be hostname or
# IPv4/IPv6 literals
subtype HostAddress => as 'Str', where { $_ =~ /^[^:]+:[0-9]+$/ }, message {
    "Address '$_' not formatted as 'hostname:port'" };
subtype HostAddressList => as 'ArrayRef[HostAddress]', message {
    "Address list <@$_> is not all hostname:port pairs" };

coerce ArrayOfHashRef => from 'HashRef', via { [$_] };
coerce DBRefColl => from 'MongoDBCollection' => via { $_->name };
coerce DBRefDB   => from 'MongoDBDatabase'   => via { $_->name };
coerce HostAddress => from 'Str', via { /:/ ? $_ : "$_:27017" };
coerce HostAddressList => from 'ArrayRef' => via { [ map { /:/ ? $_ : "$_:27017" } @$_ ] };

no Moose::Util::TypeConstraints;

1;
