#  Copyright 2012 - present MongoDB, Inc.
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
package MongoDB::BSON::Binary;

# ABSTRACT: (DEPRECATED) MongoDB binary type

use version;
our $VERSION = 'v2.1.3';

use Moo;
extends 'BSON::Bytes';

use namespace::clean -except => 'meta';

# Kept for backwards compatibilty
use constant {
    SUBTYPE_GENERIC            => 0,
    SUBTYPE_FUNCTION           => 1,
    SUBTYPE_GENERIC_DEPRECATED => 2,
    SUBTYPE_UUID_DEPRECATED    => 3,
    SUBTYPE_UUID               => 4,
    SUBTYPE_MD5                => 5,
    SUBTYPE_USER_DEFINED       => 128
};

with $_ for qw(
  MongoDB::Role::_DeprecationWarner
);

sub BUILD {
    my $self = shift;
    $self->_warn_deprecated_class(__PACKAGE__, ["BSON::Bytes"], 0);
};

1;

__END__

=head1 DESCRIPTION

This class is now an empty subclass of L<BSON::Bytes>.

=cut
