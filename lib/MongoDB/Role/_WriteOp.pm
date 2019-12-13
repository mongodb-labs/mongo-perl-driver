#  Copyright 2014 - present MongoDB, Inc.
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

use strict;
use warnings;
package MongoDB::Role::_WriteOp;

# MongoDB interface for database write operations (whether write commands
# or other things that take a write concern)

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::_Types qw(
    WriteConcern
);

use namespace::clean;

has write_concern => (
    is       => 'ro',
    required => 1,
    isa => WriteConcern,
);

sub _should_use_acknowledged_write {
    my $self = shift;

    # We should never use an unacknowledged write concern in an active transaction
    return 1 if $self->session && $self->session->_active_transaction;
    return $self->write_concern->is_acknowledged;
}

1;
