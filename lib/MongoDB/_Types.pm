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

package MongoDB::_Types;

# MongoDB Moose type definitions

use version;
our $VERSION = 'v0.704.5.1';

use Moose::Util::TypeConstraints;

class_type 'IxHash'            => { class => 'Tie::IxHash' };
class_type 'MongoDBCollection' => { class => 'MongoDB::Collection' };
class_type 'MongoDBDatabase'   => { class => 'MongoDB::Database' };

subtype ArrayOfHashRef => as 'ArrayRef[HashRef]';
subtype DBRefColl      => as 'Str';
subtype DBRefDB        => as 'Str';
subtype SASLMech       => as 'Str', where { /^GSSAPI|PLAIN$/ };

coerce ArrayOfHashRef => from 'HashRef', via { [$_] };
coerce DBRefColl => from 'MongoDBCollection' => via { $_->name };
coerce DBRefDB   => from 'MongoDBDatabase'   => via { $_->name };

no Moose::Util::TypeConstraints;

1;
