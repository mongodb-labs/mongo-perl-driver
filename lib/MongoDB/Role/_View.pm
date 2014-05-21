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

package MongoDB::Role::_View;

# ABSTRACT: MongoDB view role

use version;
our $VERSION = 'v0.703.5'; # TRIAL

use MongoDB::_Types;
use Moose::Role;
use namespace::clean -except => 'meta';

=attr query (required)

A hash reference containing a MongoDB query document

=cut

has query => (
    is       => 'ro',
    isa      => 'HashRef|IxHash',
    required => 1
);

=attr op_queue (required)

An object implementing L<MongoDB::Role::_OpQueue>

=cut

has op_queue => (
    is       => 'ro',
    does     => 'MongoDB::Role::_OpQueue',
    required => 1,
    handles  => ['_enqueue_op'],
);

1;
