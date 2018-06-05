#  Copyright 2016 - present MongoDB, Inc.
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
package MongoDB::Role::_OpReplyParser;

# MongoDB interface for sending OP_QUERY|OP_GETMORE and parsing OP_REPLY

use version;
our $VERSION = 'v1.999.0';

use Moo::Role;

use MongoDB::Error;
use MongoDB::_Protocol;
use MongoDB::_Constants;

use namespace::clean;

with $_ for qw(
  MongoDB::Role::_CommandMonitoring
);

requires '_as_command';

# Sends a BSON query/get-more string, then read, parse and validate the reply.
# Throws various errors if the results indicate a problem.  Returns
# a "result" structure generated by MongoDB::_Protocol, but with
# the 'docs' field replaced with inflated documents.

# as this is the hot loop, we do a number of odd things in the name of
# optimization, such as chaining lots of operations with ',' to keep them
# in a single statement

sub _query_and_receive {
    my ($self, $link, $op_bson, $request_id) = @_;

    my ($result, $doc_bson, $bson_codec, $docs, $len, $i);

    $self->publish_command_started( $link, $self->_as_command, $request_id )
      if $self->monitoring_callback;

    eval {
        $link->write( $op_bson ),
        ( $result   = MongoDB::_Protocol::parse_reply( $link->read, $request_id ) ),
        ( $doc_bson = $result->{docs} ),
        ( $docs     = $result->{docs} = [] ),
        ( ( $bson_codec, $i ) = ( $self->bson_codec, 0 ) ),
        ( $#$docs = $result->{number_returned} - 1 );

        # XXX should address be added to result here?

        MongoDB::CursorNotFoundError->throw("cursor not found")
        if $result->{flags}{cursor_not_found};

        # XXX eventually, BSON needs an API to do this efficiently for us without a
        # loop here.  Alternatively, BSON strings could be returned as objects that
        # inflate lazily

        while ( length($doc_bson) ) {
            $len = unpack( P_INT32, $doc_bson );
            MongoDB::ProtocolError->throw("document in response at index $i was truncated")
            if $len > length($doc_bson);
            $docs->[ $i++ ] = $bson_codec->decode_one( substr( $doc_bson, 0, $len, '' ) );
        }

        MongoDB::ProtocolError->throw(
            sprintf(
                "unexpected number of documents: got %s, expected %s",
                scalar @$docs,
                $result->{number_returned}
            )
        ) if scalar @$docs != $result->{number_returned};
    };
    if ( my $err = $@ ) {
        $self->publish_command_exception($err) if $self->monitoring_callback;
        die $err;
    }

    if ($result->{flags}{query_failure}) {
        $self->publish_legacy_query_error( $result->{docs}[0] )
          if $self->monitoring_callback;
        # had query_failure, so pretend the query was a command and assert it here
        MongoDB::CommandResult->_new(
            output  => $result->{docs}[0],
            address => $link->address
        )->assert;
    }

    $self->publish_legacy_reply_succeeded($result)
      if $self->monitoring_callback;

    return $result
}

1;
