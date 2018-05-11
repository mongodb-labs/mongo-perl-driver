#
#  Copyright 2015 MongoDB, Inc.
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
package MongoDB::Role::_BypassValidation;

# MongoDB interface for optionally applying bypassDocumentValidation
# to a command

use version;
our $VERSION = 'v1.999.0';

use Moo::Role;

use Types::Standard qw(
  Bool
);
use boolean;

use namespace::clean;

has bypassDocumentValidation => (
    is  => 'ro',
    isa => Bool
);

# args not unpacked for efficiency; args are self, validation supported
# flag, original command; returns (possibly modified) command
sub _maybe_bypass {
    push @{ $_[2] },
      bypassDocumentValidation => ( $_[0]->bypassDocumentValidation ? true : false )
      if $_[1] && defined $_[0]->bypassDocumentValidation;
    return $_[2];
}

1;
