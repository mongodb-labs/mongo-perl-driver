#  Copyright 2018 - present MongoDB, Inc.
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

package MongoDBTest::Callback;

use Moo;
use Storable qw/ dclone /;

has events => (
  is => 'lazy',
  default => sub { [] },
  clearer => 1,
);

sub callback {
  my $self = shift;
  return sub { push @{ $self->events }, dclone $_[0] };
}

sub count {
  my $self = shift;
  return scalar( @{ $self->events } );
}

1;
