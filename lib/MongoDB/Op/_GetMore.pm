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

package MongoDB::Op::_GetMore;

# Encapsulate a cursor fetch operation; returns raw results object
# (after inflation from BSON)

use version;
our $VERSION = 'v0.999.999.7';

use Moo;

use MongoDB::_Constants;
use Types::Standard qw(
    Any
    InstanceOf
    Num
    Str
);
use MongoDB::_Protocol;
use namespace::clean;

has ns => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

has cursor_id => (
    is       => 'ro',
    required => 1,
    isa      => Any,
);

has batch_size => (
    is       => 'ro',
    required => 1,
    isa      => Num,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseOp
);

sub execute {
    my ( $self, $link ) = @_;

    my ( $op_bson, $request_id ) = MongoDB::_Protocol::write_get_more( map { $self->$_ }
          qw/ns cursor_id batch_size/ );

    my $result =
      $self->_query_and_receive( $link, $op_bson, $request_id, $self->bson_codec );

    $result->{address} = $link->address;

    return $result;
}

1;
