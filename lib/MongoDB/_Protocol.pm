#  Copyright 2014 - present MongoDB, Inc.
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

use v5.8.0;
use strict;
use warnings;

package MongoDB::_Protocol;

use version;
our $VERSION = 'v2.0.1';

use MongoDB::_Constants;
use MongoDB::Error;

use Compress::Zlib ();

use constant {
    OP_REPLY        => 1,    # Reply to a client request. responseTo is set
    OP_UPDATE       => 2001, # update document
    OP_INSERT       => 2002, # insert new document
    RESERVED        => 2003, # formerly used for OP_GET_BY_OID
    OP_QUERY        => 2004, # query a collection
    OP_GET_MORE     => 2005, # Get more data from a query. See Cursors
    OP_DELETE       => 2006, # Delete documents
    OP_KILL_CURSORS => 2007, # Tell database client is done with a cursor
    OP_COMPRESSED   => 2012, # wire compression
    OP_MSG          => 2013, # generic bi-directional op code
};

use constant {
    PERL58           => $] lt '5.010',
    MIN_REPLY_LENGTH => 4 * 5 + 8 + 4 * 2,
    MAX_REQUEST_ID   => 2**31 - 1,
};

# Perl < 5.10, pack doesn't have endianness modifiers, and the MongoDB wire
# protocol mandates little-endian order. For 5.10, we can use modifiers but
# before that we only work on platforms that are natively little-endian.  We
# die during configuration on big endian platforms on 5.8

use constant {
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
    P_COMPRESSED   => PERL58 ? "l6C"     : "l<6C",
    P_MSG          => PERL58 ? "l5"      : "l<5",
    P_MSG_PL_1     => PERL58 ? "lZ*"     : "l<Z*",
};

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
#     my $msg = pack( P_INSERT, 0, int(rand(2**32-1)), 0, OP_INSERT, 0, $ns ) . $bson_docs;
#     substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );

use constant {
    # length for MsgHeader
    P_HEADER_LENGTH =>
        length(pack P_HEADER, 0, 0, 0, 0),
    # length for OP_COMPRESSED
    P_COMPRESSED_PREFIX_LENGTH =>
        length(pack P_COMPRESSED, 0, 0, 0, 0, 0, 0, 0),
    P_MSG_PREFIX_LENGTH =>
        length(pack P_MSG, 0, 0, 0, 0, 0),
};

# struct OP_MSG {
#     MsgHeader header;             // standard message header, with opCode 2013
#     uint32    flagBits;
#     Section+  sections;
#     [uint32   checksum;]
# };
#
# struct Section {
#     uint8 payloadType;
#     union payload {
#         document document;        // payloadType == 0
#         struct   sequence {       // payloadType == 1
#             int32     size;
#             cstring   identifier;
#             document* documents;
#         };
#     };
# };

use constant {
    P_SECTION_PAYLOAD_TYPE  => "C",
    P_SECTION_SEQUENCE_SIZE => PERL58 ? "l" : "l<",
};

use constant {
    P_SECTION_HEADER               => P_SECTION_PAYLOAD_TYPE . P_SECTION_SEQUENCE_SIZE,
    P_SECTION_PAYLOAD_TYPE_LENGTH  => length( pack P_SECTION_PAYLOAD_TYPE, 0 ),
    P_SECTION_SEQUENCE_SIZE_LENGTH => length( pack P_SECTION_SEQUENCE_SIZE, 0 ),
};

# TODO this really isnt the place for this...
sub maybe_split_payload {
  my ( $codec, $cmd ) = @_;

  # TODO This is probably going to explode
  my %th_cmd;
  tie %th_cmd, "Tie::IxHash", @$cmd;

  my $split_commands = {
    insert => 'documents',
    update => 'updates',
    delete => 'deletes',
  };

  my $split_docs = $split_commands->{$cmd->[0]};
  if ( defined $split_docs ) {
    # can remap documents
    my $docs = delete $th_cmd{ $split_docs };

    my $packed_pl_1 = pack ( P_MSG_PL_1, 0, $split_docs )
      . join( '', ( map { $codec->encode_one( $_ ) } @$docs ) );

    substr( $packed_pl_1, 0, 4, pack( P_INT32, length ($packed_pl_1) ) );

    $packed_pl_1 = pack( 'C', 1 ) . $packed_pl_1;

    my $packed_pl_0 = $codec->encode_one( \%th_cmd );

    $packed_pl_0 = pack( 'C', 0 ) . $packed_pl_0;

    return [ $packed_pl_0, $packed_pl_1 ];
  }

  # Drop through if nothings happened yet
  return [ $codec->encode_one( $cmd ) ];
};

=method encode_section( $payload_type, $identifier, @documents )

  MongoDB::_Protocol::encode_section( 0, undef, $doc );
  MongoDB::_Protocol::encode_section( 1, $identifier, $doc, $doc2, $doc3 );

Encodes a section for C<OP_MSG> as needed for each type of payload. Note that
payload type 0 only accepts one document, an exception will be thrown if more
than one is passed.

In a payload type 0, the identifier is not required (and is ignored). In a
payload type 1, this is used as the identifier in the sequence for the struct.

=cut

sub encode_section {
  my ( $type, $ident, @docs ) = @_;

  my $pl;

  if ( $type == 0 ) {
    MongoDB::ProtocolError->throw(
      "Creating an OP_MSG Section Payload 0 with multiple documents is not supported")
      if scalar( @docs ) > 1;
    $pl = $docs[0];
  } elsif ( $type == 1 ) {
    # Add size and ident placeholders
    $pl = pack( P_MSG_PL_1, 0, $ident )
      . join( '', @docs );
    # calculate size
    substr( $pl, 0, 4, pack( P_SECTION_SEQUENCE_SIZE, length( $pl ) ) );
  } else {
    MongoDB::ProtocolError->throw("Encode: Unsupported section payload type");
  }

  # Add payload type prefix
  $pl = pack( P_SECTION_PAYLOAD_TYPE, $type ) . $pl;

  return $pl;
}

=method decode_section( $encoded )

Peforms the exact oposite of encode_section - takes an encoded section and
returns type, identifier (if applicable) and the documents contained.

=cut

sub decode_section {
  my $enc = shift;
  my ( $type, $ident, @docs );

  # first, extract the type
  ( $type ) = unpack( 'C', $enc );
  my $payload = substr( $enc, P_SECTION_PAYLOAD_TYPE_LENGTH );

  if ( $type == 0 ) {
    # payload is actually the document
    push @docs, $payload;
  } elsif ( $type == 1 ) {
    # Pull size off and double check
    my ( $pl_size ) = unpack( P_SECTION_SEQUENCE_SIZE, $payload );
    unless ( $pl_size == length( $payload ) ) {
      MongoDB::ProtocolError->throw("Decode: Section size incorrect");
    }
    $payload = substr( $payload, P_SECTION_SEQUENCE_SIZE_LENGTH );
    # Pull out then remove
    ( $ident ) = unpack( 'Z*', $payload );
    $payload = substr( $payload, length ( pack 'Z*', $ident ) );

    while ( length $payload ) {
      my $doc_size = unpack( P_SECTION_SEQUENCE_SIZE, $payload );
      my $doc = substr( $payload, 0, $doc_size );
      $payload = substr( $payload, $doc_size );
      push @docs, $doc;
    }
  } else {
    MongoDB::ProtocolError->throw("Decode: Unsupported section payload type");
  }

  return ( $type, $ident, @docs );
}

=method join_sections

  MongoDB::_Protocol::join_sections(
    [ 0, undef, $doc ], [ 1, 'documents', $doc, $doc2 ] );

Joins an array of sections ready for passing to encode_sections.

=cut

sub join_sections {
  my ( @sections ) = @_;

  my $msg = join ('', ( map { encode_section( @$_ ) } @sections ) );

  return $msg;
}

=method split_sections( $msg )

Does the exact opposite of join_sections.

=cut

sub split_sections {
  my $msg = shift;
  my @sections;
  while ( length $msg ) {
    # get first section length
    my ( undef, $section_length ) = unpack( P_SECTION_HEADER, $msg );

    # Add the payload type length as we reached over it for the length
    my $section = substr( $msg, 0, $section_length + P_SECTION_PAYLOAD_TYPE_LENGTH );
    push @sections, [ decode_section( $section ) ];

    $msg = substr( $msg, $section_length + P_SECTION_PAYLOAD_TYPE_LENGTH );
  }

  return @sections;
}

use constant {
  MSG_FB_CHECKSUM => 0,
  MSG_FB_MORE_TO_COME => 1,
};

sub write_msg {
  my ( $sections, $flags ) = @_;
  my $flagbits = 0;
  # checksum is reserved for future use
  if ( $flags ) {
    $flagbits =
        ( $flags->{checksum_present} ? 1 << MSG_FB_CHECKSUM     : 0 )
      | ( $flags->{more_to_come}     ? 1 << MSG_FB_MORE_TO_COME : 0 );
  }

  my $request_id = int( rand( MAX_REQUEST_ID ) );

  my $msg = pack( P_MSG, 0, $request_id, 0, OP_MSG, 0 )
    . join ( '', @$sections );
  substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
  return ( $msg, $request_id );
}

# struct OP_COMPRESSED {
#     MsgHeader header;             // standard message header
#     int32_t   originalOpcode;     // wrapped op code
#     int32_t   uncompressedSize;   // size of deflated wo. header
#     uint8_t   compressorId;       // compressor
#     char*     compressedMessage;  // compressed contents
# };

# decompressors indexed by ID.
my @DECOMPRESSOR = (
    # none
    sub { shift },
    # snappy (unsupported)
    undef,
    # zlib
    sub { Compress::Zlib::uncompress(shift) },
);

# construct compressor by name with options
sub get_compressor {
    my ($name, $comp_opt) = @_;

    if ($name eq 'none') {
        return {
            id => 0,
            callback => sub { shift },
        };
    }
    elsif ($name eq 'zlib') {
        my $level = $comp_opt->{zlib_compression_level};
        $level = undef
            if defined $level and $level < 0;
        return {
            id => 2,
            callback => sub {
                return Compress::Zlib::compress(
                    $_[0],
                    defined($level) ? $level : Compress::Zlib::Z_DEFAULT_COMPRESSION(),
                );
            },
        };
    }
    else {
        MongoDB::ProtocolError->throw("Unknown compressor '$name'");
    }
}

# compress message
sub compress {
    my ($msg, $compressor) = @_;

    my ($len, $request_id, $response_to, $op_code)
        = unpack(P_HEADER, $msg);

    $msg = substr $msg, P_HEADER_LENGTH;

    my $msg_comp = pack(
        P_COMPRESSED,
        0, $request_id, $response_to, OP_COMPRESSED,
        $op_code,
        length($msg),
        $compressor->{id},
    ).$compressor->{callback}->($msg);

    substr($msg_comp, 0, 4, pack(P_INT32, length($msg_comp)));
    return $msg_comp;
}

# attempt to uncompress message
# messages that aren't OP_COMPRESSED are returned as-is
sub try_uncompress {
    my ($msg) = @_;

    my ($len, $request_id, $response_to, $op_code, $orig_op_code, $orig_len, $comp_id)
        = unpack(P_COMPRESSED, $msg);

    return $msg
        if $op_code != OP_COMPRESSED;

    $msg = substr $msg, P_COMPRESSED_PREFIX_LENGTH;

    my $decompressor = $DECOMPRESSOR[$comp_id]
        or MongoDB::ProtocolError->throw("Unknown compressor ID '$comp_id'");

    my $decomp_msg = $decompressor->($msg);
    my $done =
        pack(P_HEADER, $orig_len, $request_id, $response_to, $orig_op_code)
        .$decomp_msg;

    return $done;

}

# struct OP_UPDATE {
#     MsgHeader header;             // standard message header
#     int32     ZERO;               // 0 - reserved for future use
#     cstring   fullCollectionName; // "dbname.collectionname"
#     int32     flags;              // bit vector. see below
#     document  selector;           // the query to select the document
#     document  update;             // specification of the update to perform
# }

use constant {
    U_UPSERT       => 0,
    U_MULTI_UPDATE => 1,
};

sub write_update {
    my ( $ns, $selector, $update, $flags ) = @_;
    utf8::encode($ns);

    my $request_id = int( rand( MAX_REQUEST_ID ) );

    my $bitflags = 0;
    if ($flags) {
        $bitflags =
          ( $flags->{upsert} ? 1 << U_UPSERT : 0 )
          | ( $flags->{multi} ? 1 << U_MULTI_UPDATE : 0 );
    }

    my $msg =
        pack( P_UPDATE, 0, $request_id, 0, OP_UPDATE, 0, $ns, $bitflags )
      . $selector
      . $update;
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg, $request_id;
}

# struct OP_INSERT {
#     MsgHeader header;             // standard message header
#     int32     flags;              // bit vector - see below
#     cstring   fullCollectionName; // "dbname.collectionname"
#     document* documents;          // one or more documents to insert into the collection
# }

use constant { I_CONTINUE_ON_ERROR => 0, };

sub write_insert {
    my ( $ns, $bson_docs, $flags ) = @_;
    utf8::encode($ns);

    my $request_id = int( rand( MAX_REQUEST_ID ) );

    my $bitflags = 0;
    if ($flags) {
        $bitflags = ( $flags->{continue_on_error} ? 1 << I_CONTINUE_ON_ERROR : 0 );
    }

    my $msg =
      pack( P_INSERT, 0, $request_id, 0, OP_INSERT, $bitflags, $ns )
      . $bson_docs;
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg, $request_id;
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

use constant {
    Q_TAILABLE          => 1,
    Q_SLAVE_OK          => 2,
    Q_NO_CURSOR_TIMEOUT => 4,
    Q_AWAIT_DATA        => 5,
    Q_EXHAUST           => 6, # unsupported (PERL-282)
    Q_PARTIAL           => 7,
};

sub write_query {
    my ( $ns, $query, $fields, $skip, $batch_size, $flags ) = @_;

    utf8::encode($ns);

    my $bitflags = 0;
    if ($flags) {
        $bitflags =
            ( $flags->{tailable}   ? 1 << Q_TAILABLE          : 0 )
          | ( $flags->{slave_ok}   ? 1 << Q_SLAVE_OK          : 0 )
          | ( $flags->{await_data} ? 1 << Q_AWAIT_DATA        : 0 )
          | ( $flags->{immortal}   ? 1 << Q_NO_CURSOR_TIMEOUT : 0 )
          | ( $flags->{partial}    ? 1 << Q_PARTIAL           : 0 );
    }

    my $request_id = int( rand( MAX_REQUEST_ID ) );

    my $msg =
        pack( P_QUERY, 0, $request_id, 0, OP_QUERY, $bitflags, $ns, $skip, $batch_size )
      . $query
      . ( defined $fields && length $fields ? $fields : '' );
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return ( $msg, $request_id );
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
    my ( $ns, $cursor_id, $batch_size ) = @_;
    utf8::encode($ns);
    my $request_id = int( rand( MAX_REQUEST_ID ) );
    my $msg =
      pack( P_GET_MORE, 0, $request_id, 0, OP_GET_MORE, 0, $ns, $batch_size,
        _pack_cursor_id($cursor_id) );
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

use constant { D_SINGLE_REMOVE => 0, };

sub write_delete {
    my ( $ns, $selector, $flags ) = @_;
    utf8::encode($ns);

    my $request_id = int( rand( MAX_REQUEST_ID ) );

    my $bitflags = 0;
    if ($flags) {
        $bitflags = ( $flags->{just_one} ? 1 << D_SINGLE_REMOVE : 0 );
    }

    my $msg =
      pack( P_DELETE, 0, $request_id, 0, OP_DELETE, 0, $ns, $bitflags )
      . $selector;
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg, $request_id;
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

sub write_kill_cursors {
    my (@cursors) = map _pack_cursor_id($_), @_;

    my $request_id = int( rand( MAX_REQUEST_ID ) );

    my $msg = pack( P_KILL_CURSORS,
        0, $request_id,
        0, OP_KILL_CURSORS, 0, scalar(@cursors), @cursors );
    substr( $msg, 0, 4, pack( P_INT32, length($msg) ) );
    return $msg, $request_id;
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
    R_CURSOR_NOT_FOUND => 0,
    R_QUERY_FAILURE    => 1,
    R_AWAIT_CAPABLE    => 3,
};

sub parse_reply {
    my ( $msg, $request_id ) = @_;

    MongoDB::ProtocolError->throw("response was truncated")
        if length($msg) < MIN_REPLY_LENGTH;

    $msg = try_uncompress($msg);

    my (
        $len, $msg_id, $response_to, $opcode, $bitflags, $cursor_id, $starting_from,
        $number_returned
    ) = unpack( P_REPLY_HEADER, $msg );

    # pre-check all conditions using a modifier in one statement for speed;
    # disambiguate afterwards only if an error exists

    do {

        if ( length($msg) < $len ) {
            MongoDB::ProtocolError->throw("response was truncated");
        }

        if ( $opcode != OP_REPLY ) {
            MongoDB::ProtocolError->throw("response was not OP_REPLY");
        }

        if ( $response_to != $request_id ) {
            MongoDB::ProtocolError->throw(
                "response ID ($response_to) did not match request ID ($request_id)");
        }
        }
        if ( length($msg) < $len )
        || ( $opcode != OP_REPLY )
        || ( $response_to != $request_id );

    # returns non-zero cursor_id as blessed object to identify it as an
    # 8-byte opaque ID rather than an ambiguous Perl scalar. N.B. cursors
    # from commands are handled differently: they are perl integers or
    # else Math::BigInt objects

    substr( $msg, 0, MIN_REPLY_LENGTH, '' ),
        return {
        flags => {
            cursor_not_found => vec( $bitflags, R_CURSOR_NOT_FOUND, 1 ),
            query_failure    => vec( $bitflags, R_QUERY_FAILURE,    1 ),
        },
        cursor_id => (
            ( $cursor_id eq CURSOR_ZERO )
            ? 0
            : bless( \$cursor_id, "MongoDB::_CursorID" )
          ),
        starting_from   => $starting_from,
        number_returned => $number_returned,
        docs            => $msg,
        };
}

#--------------------------------------------------------------------------#
# utility functions
#--------------------------------------------------------------------------#

# CursorID's can come in 3 forms:
#
# 1. MongoDB::CursorID object (a blessed reference to an 8-byte string)
# 2. A perl scalar (an integer)
# 3. A Math::BigInt object (64 bit integer on 32-bit perl)
#
# The _pack_cursor_id function converts any of them to a packed Int64 for
# use in OP_GET_MORE or OP_KILL_CURSORS
sub _pack_cursor_id {
    my $cursor_id = shift;
    if ( ref($cursor_id) eq "MongoDB::_CursorID" ) {
        $cursor_id = $$cursor_id;
    }
    elsif ( ref($cursor_id) eq "Math::BigInt" ) {
        my $as_hex = $cursor_id->as_hex; # big-endian hex
        substr( $as_hex, 0, 2, '' );     # remove "0x"
        my $len = length($as_hex);
        substr( $as_hex, 0, 0, "0" x ( 16 - $len ) ) if $len < 16; # pad to quad length
        $cursor_id = pack( "H*", $as_hex );                        # packed big-endian
        $cursor_id = reverse($cursor_id);                          # reverse to little-endian
    }
    elsif (HAS_INT64) {
        # pack doesn't have endianness modifiers before perl 5.10.
        # We die during configuration on big-endian platforms on 5.8
        $cursor_id = pack( $] lt '5.010' ? "q" : "q<", $cursor_id );
    }
    else {
        # we on 32-bit perl *and* have a cursor ID that fits in 32 bits,
        # so pack it as long and pad out to a quad
        $cursor_id = pack( $] lt '5.010' ? "l" : "l<", $cursor_id ) . ( "\0" x 4 );
    }

    return $cursor_id;
}

1;

# vim: ts=4 sts=4 sw=4 et:
