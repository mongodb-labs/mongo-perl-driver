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

package MongoDB::Role::_Writeable;

# Role providing a write queue

use version;
our $VERSION = 'v0.704.5.1';

use Moose::Role;
use namespace::clean -except => 'meta';

# An object that does MongoDB::Role::_WriteQueue  This is used for
# executing write operations.

has write_queue => (
    is       => 'ro',
    does     => 'MongoDB::Role::_WriteQueue',
    required => 1,
);

# can't use 'handles' in attribute because role composition fails, so we
# do it long-hand below
sub _enqueue_write {
    my $self = shift;
    return $self->write_queue->_enqueue_write(@_);
}

1;
