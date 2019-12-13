#  Copyright 2016 - present MongoDB, Inc.
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
package MongoDB::Role::_CollectionOp;

# MongoDB role for things that operate on collections and need
# collection name (and possibly full-name for legacy operation)

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::_Types qw(
    Stringish
);

use namespace::clean;

has coll_name => (
    is       => 'ro',
    required => 1,
    isa      => Stringish,
);

has full_name => (
    is       => 'ro',
    required => 1,
    isa      => Stringish,
);

with $_ for qw(
  MongoDB::Role::_DatabaseOp
);

1;
