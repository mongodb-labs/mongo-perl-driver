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
package MongoDB::Role::_ReadOp;

# MongoDB role for read ops that provides read preference

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::_Types qw(
    ReadPreference
    ReadConcern
);
use Types::Standard qw(
    Maybe
);

use namespace::clean;

# PERL-573 Would like to refactor to remove Maybe types for
# read_preference and read_concern
has read_preference => (
    is  => 'rw', # rw for Op::_Query which can be modified by Cursor
    required => 1,
    isa => ReadPreference,
);

has read_concern => (
    is  => 'ro',
    required => 1,
    isa => ReadConcern,
);

1;
