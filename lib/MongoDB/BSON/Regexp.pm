#  Copyright 2013 - present MongoDB, Inc.
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
package MongoDB::BSON::Regexp;

# ABSTRACT: (DEPRECATED) Regular expression type

use version;
our $VERSION = 'v1.999.0';

use Moo;
extends 'BSON::Regex';

with $_ for qw(
  MongoDB::Role::_DeprecationWarner
);

sub BUILD {
    my $self = shift;
    $self->_warn_deprecated_class(__PACKAGE__, ["BSON::Regex"], 0);
};

1;

__END__

=head1 DESCRIPTION

This class is now an empty subclass of L<BSON::Regex>.

=cut

# vim: set ts=4 sts=4 sw=4 et tw=75:
