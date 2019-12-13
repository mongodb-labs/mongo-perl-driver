#  Copyright 2015 - present MongoDB, Inc.
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
package MongoDB::Role::_PrivateConstructor;

# MongoDB interface for a private constructor

use version;
our $VERSION = 'v2.2.2';

use MongoDB::_Constants;
use Sub::Defer;

use Moo::Role;

# When assertions are enabled, the private constructor delegates to the
# public one, which checks required/isa assertions.  When disabled,
# the private constructor blesses args directly to the class for speed.
BEGIN {
  my $NO_ASSERT_CONSTRUCTOR = <<'HERE';
    my %done;
    sub _new {
      my $class = shift;
      undefer_sub($class->can(q{new})) and $done{$class}++
        unless $done{$class};
      return bless {@_}, $class
    }
HERE

  WITH_ASSERTS
  ? eval 'sub _new { my $class = shift; $class->new(@_) }' ## no critic
  : eval $NO_ASSERT_CONSTRUCTOR; ## no critic
}

1;
