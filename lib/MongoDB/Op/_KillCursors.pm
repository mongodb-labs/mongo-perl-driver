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

package MongoDB::Op::_KillCursors;

# Encapsulate a cursor kill operation; returns true

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use Moose;

use MongoDB::_Types -types;
use Types::Standard -types;
use MongoDB::_Protocol;
use namespace::clean -except => 'meta';

has cursor_ids => (
    is       => 'ro',
    isa      => ArrayRef[Str],
    required => 1,
);

sub execute {
    my ( $self, $link ) = @_;

    $link->write( MongoDB::_Protocol::write_kill_cursors( @{ $self->cursor_ids } ) );

    return 1;
}

1;
