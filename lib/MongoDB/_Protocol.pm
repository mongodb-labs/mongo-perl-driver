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

use v5.8.0;
use strict;
use warnings;

package MongoDB::_Protocol;

use version;
our $VERSION = 'v0.704.4.1';

use Carp ();

use constant {
    OP_REPLY        => 1,    # Reply to a client request. responseTo is set
    OP_MSG          => 1000, # generic msg command followed by a string
    OP_UPDATE       => 2001, # update document
    OP_INSERT       => 2002, # insert new document
    RESERVED        => 2003, # formerly used for OP_GET_BY_OID
    OP_QUERY        => 2004, # query a collection
    OP_GET_MORE     => 2005, # Get more data from a query. See Cursors
    OP_DELETE       => 2006, # Delete documents
    OP_KILL_CURSORS => 2007, # Tell database client is done with a cursor
};

use constant {
    PERL58           => $] lt '5.010',
    MIN_REPLY_LENGTH => 4 * 5 + 8 + 4 * 2,
    NO_CLEAN_KEYS    => 0,
    CLEAN_KEYS       => 1,
};

# Perl < 5.10, pack doesn't have endianness modifiers, and the MongoDB wire
# protocol mandates little-endian order. For 5.10, we can use modifiers but
# before that we only work on platforms that are natively little-endian.  We
# die during configuration on big endian platforms on 5.8

use constant {
    P_INT32  => PERL58 ? "l"  : "l<",
    P_HEADER => PERL58 ? "l4" : "l<4",
};

# These ops all include P_HEADER already
use constant {
    P_UPDATE       => PERL58 ? "l5Z*l"   : "l<5Z*l<",
    P_INSERT       => PERL58 ? "l5Z*"    : "l<5Z*",
    P_QUERY        => PERL58 ? "l5Z*l2"  : "l<5Z*l<2",
    P_GET_MORE     => PERL58 ? "l5Z*la8" : "l<5Z*l<a8",
    P_DELETE       => PERL58 ? "l5Z*l"   : "l<5Z*l<",
    P_KILL_CURSORS => PERL58 ? "l6(a8)*" : "l<6(a8)*",
    P_REPLY_HEADER => PERL58 ? "l5a8l2"  : "l<5a8l<2",
};

# XXX this way of seeding/reseeding request ID is a bit of a hack
# but it should reseed on forks and thread splits
{
    my $max        = 2**31 - 1;
    my $request_id = int( rand($max) );
    my $pid        = $$;

    sub _request_id {
        if ( $pid != $$ ) {
            $request_id = int( rand($max) );
        }
        my $r = $request_id;
        $request_id = ( $request_id + 1 ) % $max;
        return $r;
    }

    # XXX maybe make $request_id :shared and use lock?
    sub CLONE {
        $request_id = int( rand($max) );
    }
}

# struct MsgHeader {
#     int32   messageLength; // total message size, including this
#     int32   requestID;     // identifier for this message
#     int32   responseTo;    // requestID from the original request
#                            //   (used in reponses from db)
#     int32   opCode;        // request type - see table below
# }
#
# Approach for MsgHeader is to write a header with 0 for length, then
# fix it up after the message is constructed.  E.g.
#     my $msg = pack( P_INSERT, 0, _request_id(), 0, OP_INSERT, 0, $ns ) . $bson_docs;
#     substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );

# struct OP_UPDATE {
#     MsgHeader header;             // standard message header
#     int32     ZERO;               // 0 - reserved for future use
#     cstring   fullCollectionName; // "dbname.collectionname"
#     int32     flags;              // bit vector. see below
#     document  selector;           // the query to select the document
#     document  update;             // specification of the update to perform
# }
sub write_update {
    my ( $ns, $selector, $update, $flags ) = @_;
    utf8::encode($ns);
    my $msg =
        pack( P_UPDATE, 0, _request_id(), 0, OP_UPDATE, 0, $ns, $flags )
      . $selector
      . $update;
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg;
}

# struct {
#     MsgHeader header;             // standard message header
#     int32     flags;              // bit vector - see below
#     cstring   fullCollectionName; // "dbname.collectionname"
#     document* documents;          // one or more documents to insert into the collection
# }
sub write_insert {
    my ( $ns, $bson_docs ) = @_;
    utf8::encode($ns);
    # we don't implement flags, so pack 0 for flags
    my $msg = pack( P_INSERT, 0, _request_id(), 0, OP_INSERT, 0, $ns ) . $bson_docs;
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg;
}

# struct OP_QUERY {
#     MsgHeader header;                 // standard message header
#     int32     flags;                  // bit vector of query options.  See below for details.
#     cstring   fullCollectionName ;    // "dbname.collectionname"
#     int32     numberToSkip;           // number of documents to skip
#     int32     numberToReturn;         // number of documents to return
#                                       //  in the first OP_REPLY batch
#     document  query;                  // query object.  See below for details.
#   [ document  returnFieldsSelector; ] // Optional. Selector indicating the fields
#                                       //  to return.  See below for details.
# }
sub write_query {
    my ( $ns, $flags, $skip, $limit, $query, $fields ) = @_;
    my $info = {
        ns         => $ns,
        opts       => $flags,
        skip       => $skip,
        limit      => $limit,
        request_id => _request_id(),
    };

    utf8::encode($ns);
    my $msg =
        pack( P_QUERY, 0, $info->{request_id}, 0, OP_QUERY, $flags, $ns, $skip, $limit )
      . $query
      . ( defined $fields && length $fields ? $fields : '' );
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return ( $msg, $info );
}

# struct {
#     MsgHeader header;             // standard message header
#     int32     ZERO;               // 0 - reserved for future use
#     cstring   fullCollectionName; // "dbname.collectionname"
#     int32     numberToReturn;     // number of documents to return
#     int64     cursorID;           // cursorID from the OP_REPLY
# }

# We treat cursor_id as an opaque string so we don't have to depend
# on 64-bit integer support

sub write_get_more {
    my ( $ns, $cursor_id, $limit ) = @_;
    utf8::encode($ns);
    my $request_id = _request_id();
    my $msg =
      pack( P_GET_MORE, 0, $request_id, 0, OP_GET_MORE, 0, $ns, $limit, $cursor_id );
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return ( $msg, $request_id );
}

# struct {
#     MsgHeader header;             // standard message header
#     int32     ZERO;               // 0 - reserved for future use
#     cstring   fullCollectionName; // "dbname.collectionname"
#     int32     flags;              // bit vector - see below for details.
#     document  selector;           // query object.  See below for details.
# }
sub write_delete {
    my ( $ns, $selector, $flags ) = @_;
    utf8::encode($ns);
    my $msg =
      pack( P_DELETE, 0, _request_id(), 0, OP_DELETE, 0, $ns, $flags ) . $selector;
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg;
}

# legacy alias
{
    no warnings 'once';
    *write_remove = \&write_delete;
}

# struct {
#     MsgHeader header;            // standard message header
#     int32     ZERO;              // 0 - reserved for future use
#     int32     numberOfCursorIDs; // number of cursorIDs in message
#     int64*    cursorIDs;         // sequence of cursorIDs to close
# }
#
# We treat cursor_id as an opaque string so we don't have to depend
# on 64-bit integer support

# This implementation takes and kills only a single cursor, which
# is expected to be the common case.
sub write_kill_cursor {
    my ($cursor) = @_;
    my $msg =
      pack( P_KILL_CURSORS, 0, _request_id(), 0, OP_KILL_CURSORS, 0, 1, $cursor );
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg;
}

# struct {
#   // MessageHeader
#     int32   messageLength; // total message size, including this
#     int32   requestID;     // identifier for this message
#     int32   responseTo;    // requestID from the original request
#     int32   opCode;        // request type - see table below
#   // OP_REPLY fields
#     int32     responseFlags;  // bit vector - see details below
#     int64     cursorID;       // cursor id if client needs to do get more's
#     int32     startingFrom;   // where in the cursor this reply is starting
#     int32     numberReturned; // number of documents in the reply
#     document* documents;      // documents
# }

# We treat cursor_id as an opaque string so we don't have to depend
# on 64-bit integer support

# flag bits relevant to drivers
use constant {
    CURSOR_NOT_FOUND => 0,
    QUERY_FAILURE    => 1,
    AWAIT_CAPABLE    => 3,
};

sub parse_reply {
    my ( $msg, $request_id ) = @_;

    if ( length($msg) < MIN_REPLY_LENGTH ) {
        Carp::croak("response was truncated");
    }

    my ( $len, $msg_id, $response_to, $opcode, $flags, $cursor_id, $starting_from,
        $number_returned )
      = unpack( P_REPLY_HEADER, substr( $msg, 0, MIN_REPLY_LENGTH, '' ) );

    if ( length($msg) + MIN_REPLY_LENGTH < $len ) {
        Carp::croak("response was truncated");
    }

    if ( $opcode != OP_REPLY ) {
        Carp::croak("response was not OP_REPLY");
    }

    if ( $response_to != $request_id ) {
        Carp::croak("response to ID ($response_to) did not match request ID ($request_id)");
    }

    if ( vec( $flags, CURSOR_NOT_FOUND, 1 ) ) {
        Carp::croak("cursor not found"); # XXX should this return something without croaking?
    }

    return {
        response_flags  => $flags,
        cursor_id       => $cursor_id,
        starting_from   => $starting_from,
        number_returned => $number_returned,
        docs            => $msg,
    };
}

1;

# vim: ts=4 sts=4 sw=4 et:
