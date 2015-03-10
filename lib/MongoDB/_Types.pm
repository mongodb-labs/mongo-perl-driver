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
our $VERSION = 'v0.999.998.3'; # TRIAL

use Type::Library
  -base,
  -declare => qw(
  ArrayofHashRef
  AuthMechanism
  Booleanpm
  ConnectType
  ConnectionStr
  CursorType
  DBRefColl
  DBRefDB
  ErrorStr
  HashLike
  HostAddress
  HostAddressList
  IxHash
  MongoDBCollection
  MongoDBDatabase
  MongoDBQuery
  NonEmptyStr
  NonNegNum
  ReadPrefMode
  ReadPreference
  ReplaceDoc
  ServerType
  TopologyType
  UpdateDoc
  WriteConcern
);

use Type::Utils -all;
use Types::Standard -types;

use Scalar::Util qw/reftype/;
use boolean;
require Tie::IxHash;

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

#--------------------------------------------------------------------------#
# Type declarations (without inherited coercions)
#--------------------------------------------------------------------------#

declare ArrayOfHashRef, as ArrayRef [HashRef];

enum AuthMechanism,
  [qw/NONE DEFAULT MONGODB-CR MONGODB-X509 GSSAPI PLAIN SCRAM-SHA-1/];

class_type Booleanpm, { class => 'boolean' };

enum ConnectType, [qw/replicaSet direct none/];

declare ConnectionStr, as Str,
  where { $_ =~ /^$uri_re$/ },
  message { "Could not parse URI '$_'" };

enum CursorType, [qw/non_tailable tailable tailable_await/];

declare DBRefColl, as Str;

declare DBRefDB, as Str;

declare ErrorStr, as Str, where { $_ }; # needs a true value

declare HashLike, as Ref, where { reftype($_) eq 'HASH' };

# XXX loose address validation for now.  Host part should really be hostname or
# IPv4/IPv6 literals
declare HostAddress, as Str,
  where { $_ =~ /^[^:]+:[0-9]+$/ and lc($_) eq $_ }, message {
    "Address '$_' not formatted as 'hostname:port'"
  };

declare HostAddressList, as ArrayRef [HostAddress], message {
    "Address list <@$_> is not all hostname:port pairs"
};

class_type IxHash, { class => 'Tie::IxHash' };

declare MaybeHashRef, as Maybe[ HashRef ];

class_type MongoDBCollection, { class => 'MongoDB::Collection' };

class_type MongoDBDatabase, { class => 'MongoDB::Database' };

class_type MongoDBQuery, { class => 'MongoDB::_Query' };

declare NonEmptyStr, as Str, where { defined $_ && length $_ };

declare NonNegNum, as Num,
  where { defined($_) && $_ >= 0 },
  message { "value must be a non-negative number" };

enum ReadPrefMode,
  [qw/primary primaryPreferred secondary secondaryPreferred nearest/];

class_type ReadPreference, { class => 'MongoDB::ReadPreference' };

enum ServerType,
  [
    qw/Standalone Mongos PossiblePrimary RSPrimary RSSecondary RSArbiter RSOther RSGhost Unknown/
  ];

enum TopologyType,
  [qw/Single ReplicaSetNoPrimary ReplicaSetWithPrimary Sharded Unknown/];

class_type WriteConcern, { class => 'MongoDB::WriteConcern' };

#--------------------------------------------------------------------------#
# Coercions
#--------------------------------------------------------------------------#

coerce ArrayOfHashRef, from HashRef, via { [$_] };

coerce Booleanpm, from Any, via { boolean($_) };

coerce DBRefColl, from MongoDBCollection, via { $_->name };

coerce DBRefDB, from MongoDBDatabase, via { $_->name };

coerce ErrorStr, from Str, via { $_ || "unspecified error" };

coerce HostAddress, from Str, via { /:/ ? lc $_ : lc "$_:27017" };

coerce HostAddressList, from ArrayRef, via {
    [ map { /:/ ? lc $_ : lc "$_:27017" } @$_ ]
};

coerce ReadPrefMode, from Str, via { $_ = lc $_; s/_?preferred/Preferred/; $_ };

coerce IxHash, from HashRef, via { Tie::IxHash->new(%$_) };

coerce IxHash, from ArrayRef, via { Tie::IxHash->new(@$_) };

coerce IxHash, from HashLike, via { Tie::IxHash->new(%$_) };

coerce ReadPreference, from HashRef,
  via { require MongoDB::ReadPreference; MongoDB::ReadPreference->new($_) };

coerce ReadPreference, from Str,
  via { require MongoDB::ReadPreference; MongoDB::ReadPreference->new( mode => $_ ) };

coerce ReadPreference, from ArrayRef,
  via { MongoDB::ReadPreference->new( mode => $_->[0], tag_sets => $_->[1] ) };

coerce WriteConcern, from HashRef,
  via { require MongoDB::WriteConcern; MongoDB::WriteConcern->new($_) };

#--------------------------------------------------------------------------#
# subtypes with inherited coercions
#--------------------------------------------------------------------------#

declare ReplaceDoc, as IxHash, coercion => 1,
  where { !$_->Length || substr( $_->Keys(0), 0, 1 ) ne '$' },
  message { "replacement document ($_) must not use '\$op' style update operators" };

declare UpdateDoc, as IxHash, coercion => 1,
  where { $_->Length && substr( $_->Keys(0), 0, 1 ) eq '$' },
  message { "update document must only use '\$op' style update operators" };

1;
