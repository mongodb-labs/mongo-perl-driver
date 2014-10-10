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

use boolean;
use Moose::Util::TypeConstraints;

sub connection_uri_re {
    return qr{
            mongodb://
            (?: ([^:]*) (?: : ([^@]*) )? @ )? # [username(:password)?@]
            ([^/]*) # host1[:port1][,host2[:port2],...[,hostN[:portN]]]
            (?:
               / ([^?]*) # /[database]
                (?: [?] (.*) )? # [?options]
            )?
    }x;
}

my $uri_re = MongoDB::_Types::connection_uri_re();

enum 'AuthMechanism',
  [qw/NONE DEFAULT MONGODB-CR MONGODB-X509 GSSAPI PLAIN SCRAM-SHA-1/];

enum 'ClusterType',
  [qw/Single ReplicaSetNoPrimary ReplicaSetWithPrimary Sharded Unknown/];

enum 'ConnectType', [qw/replicaSet direct none/];

enum 'ServerType',
  [
    qw/Standalone Mongos PossiblePrimary RSPrimary RSSecondary RSArbiter RSOther RSGhost Unknown/
  ];

enum 'ReadPrefMode',
  [qw/primary primaryPreferred secondary secondaryPreferred nearest/];

class_type 'IxHash'            => { class => 'Tie::IxHash' };
class_type 'MongoDBCollection' => { class => 'MongoDB::Collection' };
class_type 'MongoDBDatabase'   => { class => 'MongoDB::Database' };
class_type 'booleanpm'         => { class => 'boolean' };
class_type 'MongoDBQuery'      => { class => 'MongoDB::_Query' };
class_type 'ReadPreference'    => { class => 'MongoDB::ReadPreference' };

subtype ArrayOfHashRef => as 'ArrayRef[HashRef]';

subtype DBRefColl => as 'Str';
subtype DBRefDB   => as 'Str';
subtype
  ConnectionStr => as 'Str',
  where { $_ =~ /^$uri_re$/ },
  message { "Could not parse URI '$_'" };

subtype NonEmptyStr => as 'Str' => where { defined $_ && length $_ };

# Error string has to be a true value
subtype ErrorStr => as 'Str' => where { $_ };

# XXX loose address validation for now.  Host part should really be hostname or
# IPv4/IPv6 literals
subtype
  HostAddress => as 'Str',
  where { $_ =~ /^[^:]+:[0-9]+$/ and lc($_) eq $_ }, message {
    "Address '$_' not formatted as 'hostname:port'"
  };

subtype
  HostAddressList => as 'ArrayRef[HostAddress]',
  message {
    "Address list <@$_> is not all hostname:port pairs"
  };

coerce ArrayOfHashRef => from 'HashRef'           => via { [$_] };
coerce DBRefColl      => from 'MongoDBCollection' => via { $_->name };
coerce DBRefDB        => from 'MongoDBDatabase'   => via { $_->name };
coerce HostAddress => from 'Str' => via { /:/ ? lc $_ : lc "$_:27017" };
coerce ReadPrefMode => from 'Str' =>
  via { $_ = lc $_; s/_?preferred/Preferred/; $_ };
coerce booleanpm => from 'Any' => via { boolean($_) };

coerce IxHash => from 'HashRef'  => via { Tie::IxHash->new(%$_) };
coerce IxHash => from 'ArrayRef' => via { Tie::IxHash->new(@$_) };
coerce IxHash => from 'Undef'    => via { Tie::IxHash->new() };

coerce MongoDBQuery => from 'HashRef'  => via { MongoDB::_Query->new( spec => $_ ) };
coerce MongoDBQuery => from 'ArrayRef' => via { MongoDB::_Query->new( spec => $_ ) };
coerce MongoDBQuery => from 'IxHash'   => via { MongoDB::_Query->new( spec => $_ ) };
coerce MongoDBQuery => from 'Undef'    => via { MongoDB::_Query->new( spec => [] ) };

coerce HostAddressList => from 'ArrayRef' => via {
    [ map { /:/ ? lc $_ : lc "$_:27017" } @$_ ]
};

coerce ReadPreference => from 'HashRef' => via { MongoDB::ReadPreference->new($_) };
coerce ReadPreference => from 'Str' =>
  via { MongoDB::ReadPreference->new( mode => $_ ) };
coerce ReadPreference => from 'ArrayRef' =>
  via { MongoDB::ReadPreference->new( mode => $_->[0], tagsets => $_->[1] ) };

coerce ErrorStr => from 'Str' => via { $_ || "unspecified error" };

no Moose::Util::TypeConstraints;

# Classes for coercions
require Tie::IxHash;
require MongoDB::_Query;
require MongoDB::ReadPreference;

1;
