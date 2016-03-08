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

package MongoDB::Role::_CommandOp;

# MongoDB interface for database command operations

use version;
our $VERSION = 'v1.3.4';

use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use Moo::Role;
use namespace::clean;

with 'MongoDB::Role::_DatabaseOp';

requires qw/db_name bson_codec/;

sub _send_command {
    my ( $self, $link, $doc, $flags ) = @_;

    my $command = $self->bson_codec->encode_one( $doc );

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $self->db_name . '.$cmd',
        $command, undef, 0, -1, $flags );

    if ( length($op_bson) > MAX_BSON_WIRE_SIZE ) {
        # XXX should this become public?
        MongoDB::_CommandSizeError->throw(
            message => "database command too large",
            size    => length $op_bson,
        );
    }

    # return a raw, parsed result, not an object
    return $self->_query_and_receive( $link, $op_bson, $request_id, undef, 1 )
      ->{docs}[0];
}

1;
