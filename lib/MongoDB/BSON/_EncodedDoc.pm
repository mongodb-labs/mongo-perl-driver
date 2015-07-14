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

package MongoDB::BSON::_EncodedDoc;

# Wrapper for pre-encoded BSON documents, with optional metadata

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use Moose;
use MongoDB::_Constants;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

# An encoded document, i.e. a BSON string
has bson => (
    is => 'ro',
    required => 1,
    ( WITH_ASSERTS ? ( isa => Str ) : () ),
);

# A hash reference of optional meta data about the document, such as the "_id"
has metadata => (
    is => 'ro',
    builder => '_build_meta',
    lazy => 1,
    ( WITH_ASSERTS ? ( isa => HashRef ) : () ),
);

sub _build_meta { return {} }

__PACKAGE__->meta->make_immutable;

1;

# vim: set ts=4 sts=4 sw=4 et tw=75:
