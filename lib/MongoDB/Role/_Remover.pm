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

package MongoDB::Role::_Remover;

# Role for remove operations

use version;
our $VERSION = 'v0.704.5.1';

use Moose::Role;
use namespace::clean -except => 'meta';

requires qw/_enqueue_write query/;

sub remove {
    my ($self) = @_;
    $self->_enqueue_write( [ delete => { q => $self->query, limit => 0 } ] );
    return;
}

sub remove_one {
    my ($self) = @_;
    $self->_enqueue_write( [ delete => { q => $self->query, limit => 1 } ] );
    return;
}

1;
