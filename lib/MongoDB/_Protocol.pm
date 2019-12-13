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
our $VERSION = 'v2.2.2';

use MongoDB::_Constants;
use MongoDB::Error;
use MongoDB::_Types qw/ to_IxHash /;

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

# Takes a command, returns sections ready for joining

sub prepare_sections {
  my ( $codec, $cmd ) = @_;

  my %split_commands = (
    insert => 'documents',
    update => 'updates',
    delete => 'deletes',
  );

  $cmd = to_IxHash( $cmd );

  # Command is always first key in cmd
  my $command = do { my @keys = $cmd->Keys; $keys[0] };
  my $ident = $split_commands{ $command };

  if ( defined $ident ) {
    my $collection = $cmd->FETCH( $command );
    my $docs = $cmd->FETCH( $ident );
    # Assumes only a single split on the commands
    return (
        {
            type => 0,
            documents => [ [
                # Done specifically to not alter $cmd.
                # The command ($command from earlier) is assumed to be
                # first in the Keys set
                map { $_ eq $ident
                    ? ()
                    : ( $_, $cmd->FETCH( $_ ) )
                } $cmd->Keys()
            ] ],
        },
        {
            type => 1,
            identifier => $ident,
            documents => $docs,
        }
    );
  } else {
    # Not a recognised command to split, just set up ready for later
    return (
        {
            type => 0,
            documents => [ $cmd ],
        }
    );
  }
}

# encode_section
#
#     MongoDB::_Protocol::encode_section( $codec, {
#         type => 0,                  # 0 or 1
#         identifier => undef,        # optional in type 0
#         documents => [ $cmd ]       # must be an array of documents
#     });
#
# Takes a section hashref and encodes it for joining

sub encode_section {
    my ( $codec, $section ) = @_;

    my $type = $section->{type};
    my $ident = $section->{identifier};
    my @docs = map { $codec->encode_one( $_ ) } @{ $section->{documents} };

    my $pl;
    if ( $type == 0 ) {
        # Assume a single doc if payload type is 0
        $pl = $docs[0];
    } elsif ( $type == 1 ) {
        $pl = pack( P_MSG_PL_1, 0, $ident )
          . join( '', @docs );
        # calculate size
        substr( $pl, 0, 4, pack( P_SECTION_SEQUENCE_SIZE, length( $pl ) ) );
    } else {
      MongoDB::ProtocolError->throw("Encode: Unsupported section payload type");
    }

    # Prepend the section type
    $pl = pack( P_SECTION_PAYLOAD_TYPE, $type ) . $pl;

    return $pl;
}

# decode_section
#
#     MongoDB::_Protocol::decode_section( $section )
#
# Takes an encoded section and decodes it, exactly the opposite of encode_section.

sub decode_section {
    my ( $doc ) = @_;
    my ( $type, $ident, @enc_docs );
    my $section = {};

    ( $type ) = unpack( 'C', $doc );
    my $payload = substr( $doc, P_SECTION_PAYLOAD_TYPE_LENGTH );

    $section->{ type } = $type;

    # Pull size off and double check. Size is in the same place regardless of
    # payload type, as its a similar struct to a raw document
    my ( $pl_size ) = unpack( P_SECTION_SEQUENCE_SIZE, $payload );
    unless ( $pl_size == length( $payload ) ) {
      MongoDB::ProtocolError->throw("Decode: Section size incorrect");
    }

    if ( $type == 0 ) {
        # payload is a raw document
        push @enc_docs, $payload;
    } elsif ( $type == 1 ) {
        $payload = substr( $payload, P_SECTION_SEQUENCE_SIZE_LENGTH );
        # Pull out then remove
        ( $ident ) = unpack( 'Z*', $payload );
        $section->{ identifier } = $ident;
        $payload = substr( $payload, length ( $ident ) + 1 ); # add one for null termination

        while ( length $payload ) {
          my $doc_size = unpack( P_SECTION_SEQUENCE_SIZE, $payload );
          my $doc = substr( $payload, 0, $doc_size );
          $payload = substr( $payload, $doc_size );
          push @enc_docs, $doc;
        }
    } else {
        MongoDB::ProtocolError->throw("Decode: Unsupported section payload type");
    }
    $section->{ documents } = \@enc_docs;

    return $section;
}

# method split_sections( $msg )
#
# Splits sections based on their payload length header. Returns an array of
# sections in packed form

sub split_sections {
  my $msg = shift;
  my @sections;
  while ( length $msg ) {
    # get first section length
    my ( undef, $section_length ) = unpack( P_SECTION_HEADER, $msg );

    # Add the payload type length as we reached over it for the length
    my $section = substr( $msg, 0, $section_length + P_SECTION_PAYLOAD_TYPE_LENGTH );

    push @sections, decode_section( $section );

    $msg = substr( $msg, $section_length + P_SECTION_PAYLOAD_TYPE_LENGTH );
  }

  return @sections;
}

use constant {
  MSG_FB_CHECKSUM => 0,
  MSG_FB_MORE_TO_COME => 1,
};

sub write_msg {
  my ( $codec, $flags, $cmd ) = @_;
  my $flagbits = 0;
  # checksum is reserved for future use
  if ( $flags ) {
    $flagbits =
        ( $flags->{checksum_present} ? 1 << MSG_FB_CHECKSUM     : 0 )
      | ( $flags->{more_to_come}     ? 1 << MSG_FB_MORE_TO_COME : 0 );
  }

  my $request_id = int( rand( MAX_REQUEST_ID ) );

  my @sections = prepare_sections( $codec, $cmd );

  my $encoded_sections = join ('', ( map { encode_section( $codec, $_ ) } @sections ) );

  my $msg = pack( P_MSG, 0, $request_id, 0, OP_MSG, 0 )
    . $encoded_sections;
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

# Note that Zlib is in perl core (since 5.9.3) so shouldnt need lazy loading
sub _assert_zstd {
    MongoDB::UsageError->throw(qq/Compress::Zstd must be installed to support zstd compression\n/)
      unless eval { require Compress::Zstd };
}

sub _assert_snappy {
    MongoDB::UsageError->throw(qq/Compress::Snappy must be installed to support snappy compression\n/)
      unless eval { require Compress::Snappy };
}

# decompressors indexed by ID.
my @DECOMPRESSOR = (
    # none
    sub { shift },
    # snappy
    sub { Compress::Snappy::decompress(shift) },
    # zlib
    sub { Compress::Zlib::uncompress(shift) },
    # zstd
    sub { Compress::Zstd::decompress(shift) },
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
    elsif ($name eq 'snappy') {
        _assert_snappy();
        return {
            id => 1,
            callback => sub { Compress::Snappy::compress(shift) },
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
    elsif ($name eq 'zstd') {
        _assert_zstd();
        return {
            id => 3,
            callback => sub { Compress::Zstd::compress(shift) },
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
            ( $flags->{upsert} ? 1 << U_UPSERT       : 0 )
          | ( $flags->{multi}  ? 1 << U_MULTI_UPDATE : 0 );
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
    ) = unpack( P_MSG, $msg );

    # pre-check all conditions using a modifier in one statement for speed;
    # disambiguate afterwards only if an error exists

    do {

        if ( length($msg) < $len ) {
            MongoDB::ProtocolError->throw("response was truncated");
        }

        if ( $opcode != OP_REPLY && $opcode != OP_MSG ) {
            MongoDB::ProtocolError->throw("response was not OP_REPLY or OP_MSG");
        }

        if ( $response_to != $request_id ) {
            MongoDB::ProtocolError->throw(
                "response ID ($response_to) did not match request ID ($request_id)");
        }
        }
        if ( length($msg) < $len )
        || ( ( $opcode != OP_REPLY ) && ( $opcode != OP_MSG ) )
        || ( $response_to != $request_id );


    if ( $opcode == OP_MSG ) {
        # XXX Extract and check checksum - future support of crc32c
        my @sections = split_sections( substr( $msg, P_MSG_PREFIX_LENGTH ) );
        # We have none of the other stuff? maybe flags... and an array of docs? erm
        return {
          flags => {
            checksum_present => vec( $bitflags, MSG_FB_CHECKSUM, 1 ),
            more_to_come    => vec( $bitflags, MSG_FB_MORE_TO_COME, 1 ),
          },
          # XXX Assumes the server never sends a type 1 payload. May change in future
          docs => $sections[0]->{documents}->[0]
        };
    } else {
        # Yes its two unpacks but its just easier than mapping through to the right size
        (
            $len, $msg_id, $response_to, $opcode, $bitflags, $cursor_id, $starting_from,
            $number_returned
        ) = unpack( P_REPLY_HEADER, $msg );
    }

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
