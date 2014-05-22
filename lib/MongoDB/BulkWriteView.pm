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

package MongoDB::BulkWriteView;

# ABSTRACT: Bulk write operations against a query document

use version;
our $VERSION = 'v0.703.5'; # TRIAL

use Moose;
use namespace::clean -except => 'meta';

# done in two parts to work around a moose bug: https://github.com/moose/Moose/pull/19
with qw(
  MongoDB::Role::_View
  MongoDB::Role::_Writeable
);

with qw(
  MongoDB::Role::_Updater
  MongoDB::Role::_Remover
);

__PACKAGE__->meta->make_immutable;

1;
