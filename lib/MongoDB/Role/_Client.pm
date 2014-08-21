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

package MongoDB::Role::_Client;

# Role implementing database operations over a socket

use version;
our $VERSION = 'v0.704.4.1';

use MongoDB::BSON;
use MongoDB::_Protocol;
use MongoDB::_Types;
use Moose::Role;
use namespace::clean -except => 'meta';

sub _send_admin_command {
    my ($self, $link, $opts, $command) = @_;
    return $self->_send_command( $link, 'admin.$cmd', $opts, $command );
}

sub _send_command {
    my ($self, $link, $ns, $opts, $command) = @_;

    my $cmd_bson = MongoDB::BSON::encode_bson( $command, 0 );
    my ($query, $info) = MongoDB::_Protocol::write_query( $ns, $opts, 0, -1, $cmd_bson );
    $link->write($query);

    my $result = MongoDB::_Protocol::parse_reply($link->read, $info->{request_id});
    my $doc_bson = $result->{docs};
    my $len = unpack( MongoDB::_Protocol::P_INT32(), substr( $doc_bson, 0, 4 ) );
    if ( $len > length($doc_bson) ) {
        Carp::croak("document in response was truncated"); # XXX ought to be done by BSON parser
    }
    return MongoDB::BSON::decode_bson( $doc_bson );
}

1;
