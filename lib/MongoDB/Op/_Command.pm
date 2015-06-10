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
our $VERSION = 'v0.999.999.2'; # TRIAL

use Moose;

use MongoDB::_Types -types;
use Types::Standard -types;
use Tie::IxHash;
use namespace::clean -except => 'meta';

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has query => (
    is       => 'ro',
    isa      => IxHash,
    coerce   => 1,
    required => 1,
    writer   => '_set_query',
);

has query_flags => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

with 'MongoDB::Role::_CommandOp';
with 'MongoDB::Role::_ReadOp';
with 'MongoDB::Role::_ReadPrefModifier';

sub execute {
    my ( $self, $link, $topology_type ) = @_;
    $topology_type ||= 'Single'; # if not specified, assume direct

    $self->_apply_read_prefs( $link, $topology_type );

    my $res = MongoDB::CommandResult->new(
        output => $self->_send_command( $link, $self->query, $self->query_flags ),
        address => $link->address
    );

    $res->assert;

    return $res;
}

1;
