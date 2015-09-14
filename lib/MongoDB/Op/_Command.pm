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

package MongoDB::Op::_Command;

# Encapsulate running a command and returning a MongoDB::CommandResult

use version;
our $VERSION = 'v1.1.0';

use Moo;

use MongoDB::_Constants;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    HashRef
    Str
);
use Tie::IxHash;
use namespace::clean;

has db_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has query => (
    is       => 'ro',
    required => 1,
    writer   => '_set_query',
    isa      => Document,
);

has query_flags => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CommandOp
  MongoDB::Role::_ReadOp
  MongoDB::Role::_LegacyReadPrefModifier
);

sub execute {
    my ( $self, $link, $topology_type ) = @_;
    $topology_type ||= 'Single'; # if not specified, assume direct

    $self->_apply_read_prefs( $link, $topology_type, $self->query_flags, \$self->query);

    my $res = MongoDB::CommandResult->_new(
        output => $self->_send_command( $link, $self->query, $self->query_flags ),
        address => $link->address
    );

    $res->assert;

    return $res;
}

1;
