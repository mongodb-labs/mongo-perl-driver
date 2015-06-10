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

package MongoDB::Role::_ReadOp;

# MongoDB role for read ops that provides read preference

use version;
our $VERSION = 'v0.999.999.2'; # TRIAL

use Moose::Role;

use MongoDB::ReadPreference;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

my $PRIMARY = MongoDB::ReadPreference->new;

with 'MongoDB::Role::_DatabaseOp';

has read_preference => (
    is      => 'ro',
    isa     => ReadPreference,
    coerce  => 1,
    default => sub { $PRIMARY },
);

1;
