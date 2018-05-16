#
#  Copyright 2016 MongoDB, Inc.
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

use strict;
use warnings;
package MongoDB::Role::_RetryableBulk;

# MongoDB role for retryable bulk commands. Stores a flag, and things that operate on collections and need
# collection name (and possibly full-name for legacy operation)

use version;
our $VERSION = 'v1.999.0';

use Moo::Role;

use Types::Standard qw(
    Bool
);

has _retryable => (
    is => 'rw',
    isa => Bool,
    default => 1,
);

1;
