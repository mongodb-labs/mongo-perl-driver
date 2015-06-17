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
our $VERSION = 'v0.999.999.3'; # TRIAL

use Moose;

use MongoDB::_Types -types;
use Types::Standard -types;
use MongoDB::_Protocol;
use namespace::clean -except => 'meta';

has ns => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has client => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

has cursor_id => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has batch_size => (
    is      => 'ro',
    isa     => Num,
    default => 0,
);

with 'MongoDB::Role::_DatabaseOp';

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
