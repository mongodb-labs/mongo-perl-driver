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

use strict;
use warnings;
package MongoDB::_Types;

# MongoDB type definitions

use version;
our $VERSION = 'v1.999.0';

use Type::Library
  -base,
  -declare => qw(
  ArrayOfHashRef
  AuthMechanism
  Booleanpm
  BSONCodec
  ClientSession
  ConnectType
  CursorType
  DBRefColl
  DBRefDB
  Document
  ErrorStr
  HashLike
  HeartbeatFreq
  HostAddress
  HostAddressList
  IndexModel
  IndexModelList
  IxHash
  MaxStalenessNum
  MaybeHashRef
  MongoDBCollection
  MongoDBDatabase
  NonEmptyStr
  NonNegNum
  OID
  OrderedDoc
  PairArrayRef
  ReadPrefMode
  ReadConcern
  ReadPreference
  ServerDesc
  ServerType
  SingleChar
  SingleKeyHash
  TopologyType
  WriteConcern
);

use Type::Utils -all;
use Types::Standard qw(
    Any
    ArrayRef
    Dict
    HashRef
    Maybe
    Num
    Optional
    Ref
    Str
    Undef
);

use Scalar::Util qw/reftype/;
use boolean 0.25;
require Tie::IxHash;

#--------------------------------------------------------------------------#
# Type declarations (without inherited coercions)
#--------------------------------------------------------------------------#

declare ArrayOfHashRef, as ArrayRef [HashRef];

enum AuthMechanism,
  [qw/NONE DEFAULT MONGODB-CR MONGODB-X509 GSSAPI PLAIN SCRAM-SHA-1 SCRAM-SHA-256/];

class_type Booleanpm, { class => 'boolean' };

duck_type BSONCodec, [ qw/encode_one decode_one/ ];

class_type ClientSession, { class => 'MongoDB::ClientSession' };

enum ConnectType, [qw/replicaSet direct none/];

enum CursorType, [qw/non_tailable tailable tailable_await/];

declare ErrorStr, as Str, where { $_ }; # needs a true value

declare HashLike, as Ref, where { reftype($_) eq 'HASH' };

declare HeartbeatFreq, as Num,
  where { defined($_) && $_ >= 500 },
  message { "value must be at least 500" };

# XXX loose address validation for now.  Host part should really be hostname or
# IPv4/IPv6 literals
declare HostAddress, as Str,
  where { $_ =~ /^[^:]+:[0-9]+$/ and lc($_) eq $_ }, message {
    "Address '$_' either not lowercased or not formatted as 'hostname:port'"
  };

declare HostAddressList, as ArrayRef [HostAddress], message {
    "Address list <@$_> not all formatted as lowercased 'hostname:port' pairs"
};

class_type IxHash, { class => 'Tie::IxHash' };

declare MaybeHashRef, as Maybe[ HashRef ];

class_type MongoDBCollection, { class => 'MongoDB::Collection' };

class_type MongoDBDatabase, { class => 'MongoDB::Database' };

declare NonEmptyStr, as Str, where { defined $_ && length $_ };

declare NonNegNum, as Num,
  where { defined($_) && $_ >= 0 },
  message { "value must be a non-negative number" };

declare MaxStalenessNum, as Num,
  where { defined($_) && ( $_ > 0 || $_ == -1 ) },
  message { "value must be a positive number or -1" };

declare OID, as Str, where { /\A[0-9a-f]{24}\z/ }, message {
    "Value '$_' is not a valid OID"
};

declare PairArrayRef, as ArrayRef,
  where { @$_ % 2 == 0 };

enum ReadPrefMode,
  [qw/primary primaryPreferred secondary secondaryPreferred nearest/];

class_type ReadPreference, { class => 'MongoDB::ReadPreference' };

class_type ReadConcern, { class => 'MongoDB::ReadConcern' };

class_type ServerDesc, { class => 'MongoDB::_Server' };

enum ServerType,
  [
    qw/Standalone Mongos PossiblePrimary RSPrimary RSSecondary RSArbiter RSOther RSGhost Unknown/
  ];

declare SingleChar, as Str, where { length $_ eq 1 };

declare SingleKeyHash, as HashRef, where { 1 == scalar keys %$_ };

enum TopologyType,
  [qw/Single ReplicaSetNoPrimary ReplicaSetWithPrimary Sharded Unknown/];

class_type WriteConcern, { class => 'MongoDB::WriteConcern' };

# after SingleKeyHash, PairArrayRef and IxHash
declare OrderedDoc, as PairArrayRef|IxHash|SingleKeyHash;
declare Document, as HashRef|PairArrayRef|IxHash|HashLike;

# after NonEmptyStr
declare DBRefColl, as NonEmptyStr;
declare DBRefDB, as NonEmptyStr|Undef;

# after OrderedDoc
declare IndexModel, as Dict [ keys => OrderedDoc, options => Optional [HashRef] ];
declare IndexModelList, as ArrayRef [IndexModel];

#--------------------------------------------------------------------------#
# Coercions
#--------------------------------------------------------------------------#

coerce ArrayOfHashRef, from HashRef, via { [$_] };

coerce BSONCodec, from HashRef,
  via { require MongoDB::BSON; MongoDB::BSON->new($_) };

coerce Booleanpm, from Any, via { boolean($_) };

coerce DBRefColl, from MongoDBCollection, via { $_->name };

coerce DBRefDB, from MongoDBDatabase, via { $_->name };

coerce ErrorStr, from Str, via { $_ || "unspecified error" };

coerce ReadPrefMode, from Str, via { $_ = lc $_; s/_?preferred/Preferred/; $_ };

coerce IxHash, from HashRef, via { Tie::IxHash->new(%$_) };

coerce IxHash, from ArrayRef, via { Tie::IxHash->new(@$_) };

coerce IxHash, from HashLike, via { Tie::IxHash->new(%$_) };

coerce OID, from Str, via { lc $_ };

coerce ReadPreference, from HashRef,
  via { require MongoDB::ReadPreference; MongoDB::ReadPreference->new($_) };

coerce ReadPreference, from Str,
  via { require MongoDB::ReadPreference; MongoDB::ReadPreference->new( mode => $_ ) };

coerce ReadPreference, from ArrayRef,
  via { require MongoDB::ReadPreference; MongoDB::ReadPreference->new( mode => $_->[0], tag_sets => $_->[1] ) };

coerce ReadConcern, from Str,
  via { require MongoDB::ReadConcern; MongoDB::ReadConcern->new( level => $_ ) };

coerce ReadConcern, from HashRef,
  via { require MongoDB::ReadConcern; MongoDB::ReadConcern->new($_) };

coerce WriteConcern, from HashRef,
  via { require MongoDB::WriteConcern; MongoDB::WriteConcern->new($_) };

1;
