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

package MongoDB::Role::_CommandCursorOp;

# MongoDB interface for database commands with cursors

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use MongoDB::Error;
use MongoDB::QueryResult;
use Moose::Role;
use Types::Standard -types;
use namespace::clean -except => 'meta';

requires 'client';

sub _build_result_from_cursor {
    my ( $self, $res ) = @_;

    my $cursor = $res->result->{cursor}
      or MongoDB::DatabaseError->throw(
        message => "no cursor found in command response",
        result  => $res,
      );

    my $qr = MongoDB::QueryResult->new(
        _client => $self->client,
        address => $res->address,
        cursor  => $cursor,
    );
}

1;
