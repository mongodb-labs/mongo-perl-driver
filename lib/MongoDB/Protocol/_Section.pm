package MongoDB::Protocol::_Section;

use Moo;
use Types::Standard qw(
    ArrayRef
    Str
    Maybe
    Enum
);
use MongoDB::_Types qw(
    BSONCodec
);
use MongoDB::Error;

use constant {
    PERL58           => $] lt '5.010',
};
use constant {
    P_MSG_PL_1     => PERL58 ? "lZ*"     : "l<Z*",
    P_SECTION_PAYLOAD_TYPE  => "C",
    P_SECTION_SEQUENCE_SIZE => PERL58 ? "l" : "l<",
};
use constant {
    P_SECTION_HEADER               => P_SECTION_PAYLOAD_TYPE . P_SECTION_SEQUENCE_SIZE,
    P_SECTION_PAYLOAD_TYPE_LENGTH  => length( pack P_SECTION_PAYLOAD_TYPE, 0 ),
    P_SECTION_SEQUENCE_SIZE_LENGTH => length( pack P_SECTION_SEQUENCE_SIZE, 0 ),
};

use namespace::clean;

has bson_codec => (
    is => 'ro',
    required => 1,
    isa => BSONCodec,
);

# either passed in on creation or pulled from binary docs
has type => (
    is => 'lazy',
    isa => Enum[ 0, 1 ],
);

sub _build_type {
    my $self = shift;
    return $self->_decoded->[0] if $self->has_binary;
    MongoDB::ProtocolError->throw('No type passed to Section');
}

# If not passed in, assume is undef (just makes this one easier)
has identifier => (
    is => 'lazy',
    isa => Maybe[Str],
);

sub _build_identifier {
    my $self = shift;
    return $self->_decoded->[1] if $self->has_binary;
    return;
}

# Either passed in, or created from the binary documents.
has documents => (
    is => 'lazy',
    isa => ArrayRef,
);

sub _build_documents {
    my $self = shift;
    my @docs;
    if ( $self->has_binary ) {
        @docs = (
            map { $self->bson_codec->decode_one( $_ ) } @{ $self->encoded_documents }
        );
    } else {
        MongoDB::ProtocolError->throw('No documents passed to Section');
    }
    return \@docs;
}

has encoded_documents => (
    is => 'lazy',
    isa => ArrayRef,
);

sub _build_encoded_documents {
    my $self = shift;
    my @docs;
    if ( $self->has_binary ) {
        ( undef, undef, @docs ) = @{ $self->_decoded };
    } else {
        @docs = (
            map { $self->bson_codec->encode_one( $_ ) } @{ $self->documents }
        );
    }
    return \@docs;
}

has binary => (
    is => 'lazy',
    isa => Str,
    predicate => 1,
);

sub _build_binary { return shift->_encoded }

has _encoded => (
    is => 'lazy',
    isa => Str,
);

sub _build__encoded {
    my $self = shift;

    my $type = $self->type;
    my $ident = $self->identifier;
    my @docs = @{ $self->encoded_documents };

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

has _decoded => (
    is => 'lazy',
    isa => ArrayRef,
);

sub _build__decoded {
    my $self = shift;
    MongoDB::ProtocolError->throw('Section requires binary to decode')
        unless $self->has_binary;
    my $enc = $self->binary;
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

    return [ $type, $ident, @docs ];
}

=head1 SYNOPSIS

    # From a known document set
    MongoDB::Protocol::_Section->new(
        bson_codec => BSON->new,
        type => 0,
        identifier => undef,
        documents => [ $doc ],
    );

    # From a binary section
    MongoDB::Protocol::_Section->new(
        bson_codec => BSON->new,
        binary => $bin,
    );

=cut

1;
