#
#  Copyright 2009-2013 MongoDB, Inc.
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

package MongoDB::BSON;


# ABSTRACT: Tools for serializing and deserializing data in BSON form

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use XSLoader;
XSLoader::load("MongoDB", $VERSION);

use Carp ();
use Config;
use if ! $Config{use64bitint}, "Math::BigInt";
use MongoDB::Error;
use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use Type::Params qw/compile/;
use namespace::clean -except => 'meta';

=attr dbref_callback

A document with keys C<$ref> and C<$id> is a special MongoDB convention
representing a
L<DBRef|http://docs.mongodb.org/manual/applications/database-references/#dbref>.

This attribute specifies a function reference that will be called with a hash
reference argument representing a DBRef.

The hash reference will have keys C<$ref> and C<$id> and may have C<$db> and
other keys.  The callback must return a scalar value representing the dbref
(e.g. a document, an object, etc.)

The default returns the DBRef hash reference without modification.

=cut

has dbref_callback => (
    is      => 'ro',
    isa     => CodeRef,
    default => sub { sub { shift }  },
);

=attr dt_type

Sets the type of object which is returned for BSON DateTime fields. The default
is L<DateTime>. Other acceptable values are L<Time::Moment>, L<DateTime::Tiny>
and C<undef>. The latter will give you the raw epoch value rather than an
object.

# XXX add MongoDB::BSON::DateTime support and make it the default

=cut

has dt_type => (
    is      => 'ro',
    isa     => Str|Undef,
    default => 'DateTime',
);

=attr error_callback

This attribute specifies a function reference that will be called with
three positional arguments:

=for :list
* an error string argument describing the error condition
* a reference to the problematic document or byte-string
* the method in which the error occurred (e.g. C<encode_one> or C<decode_one>)

Note: for decoding errors, the byte-string is passed as a reference to avoid
copying possibly large strings.

If not provided, errors messages will be thrown with C<Carp::croak>.

=cut

# XXX should this be separate for encode & decode? e.g. encode always want
# to throw with document and decode doesn't?

has error_callback => (
    is      => 'ro',
    isa     => Maybe[CodeRef],
);

=attr invalid_chars

A string containing ASCII characters that must not appear in keys.  The default
is the empty string, meaning there are no invalid characters.

=cut

has invalid_chars => (
    is      => 'ro',
    isa     => Str,
    default => '',
);

## XXX don't need this yet; do with 'invalid_chars' first for now
##
##=attr key_validator
##
##This attribute must be either a regular expresssion reference or
##a function reference to validate a key during encoding.
##
##A function reference will get a single string argument and must return
##an error string if the key is B<invalid>; if the key is B<valid> it must
##return B<nothing>.
##
##A regular expression will be matched against the key; if it matches, the
##key is valid.
##
##If not provided, any key is ok.
##
##=cut
##
##has key_validator => (
##    is      => 'ro',
##    isa     => Maybe[RegexpRef|CodeRef],
##);
##
## XXX don't need this until encode_many is implemented
##
##=attr max_batch_size
##
##This attribute defines the maximum number of documents to return in a chunk
##when using C<encode_many>.  The default is 0, which disables any maximum.
##
##=cut
##
##has max_batch_size=> (
##    is => 'ro',
##    isa => NonNegNum,
##    default => 0,
##);

## XXX don't need this until we have a back-end that isn't hard-coded
##
##=attr max_depth
##
##This attribute defines the maximum document depth allowed.  The default
##is 100 for both encoding and decoding.
##
##This
##
##=cut
##
##has max_depth => (
##    is => 'ro',
##    isa => NonNegNum,
##    default => 100,
##);

=attr max_length

This attribute defines the maximum document size. The default is 0, which
disables any maximum.

If set to a positive number, it applies to both encoding B<and> decoding (the
latter is necessary for prevention of resource consumption attacks).

=cut

has max_length => (
    is => 'ro',
    isa => NonNegNum,
    default => 0,
);

=attr op_char

This is a single character to use for special operators.  If a key starts
with C<op_char>, the C<op_char> character will be replaced with "$".

The default is "$".

=cut

has op_char => (
    is => 'ro',
    isa => Maybe[ SingleChar ],
);

=attr prefer_numeric

If set to true, scalar values that look like a numeric value will be
encoded as a BSON numeric type.  When false, if the scalar value was ever
used as a string, it will be encoded as a BSON UTF-8 string.

The default is false.

=cut

has prefer_numeric => (
    is => 'ro',
    isa => Bool,
);

#--------------------------------------------------------------------------#
# public methods
#--------------------------------------------------------------------------#

=method encode_one

    $byte_string = $codec->encode_one( $doc );
    $byte_string = $codec->encode_one( $doc, \%options );

Takes a "document", typically a hash reference, an array reference, or a
Tie::IxHash object and returns a byte string with the BSON representation of
the document.

An optional hash reference of options may be provided.  Valid options include:

=for :list
* first_key – if C<first_key> is defined, it and C<first_value>
  will be encoded first in the output BSON; any matching key found in the
  document will be ignored.
* first_value - value to assign to C<first_key>; will encode as Null if omitted
* error_callback – overrides codec default
* invalid_chars – overrides codec default
* max_length – overrides codec default
* op_char – overrides codec default
* prefer_numeric – overrides codec default

=cut


my @encode_overrides =
  qw/error_callback invalid_chars max_length op_char prefer_numeric/;
my $encode_one_args;

sub encode_one {
    $encode_one_args ||= compile( Object, Any, Optional [HashRef]);
    my ( $self, $document, $options ) = $encode_one_args->(@_);

    for my $k (@encode_overrides) {
        $options->{$k} = $self->$k unless exists $options->{$k};
    }

    my $bson = eval { MongoDB::BSON::_encode_bson( $document, $options ) };
    if ( $@ or ( $options->{max_length} && length($bson) > $options->{max_length} ) ) {
        my $msg = $@ || "Document exceeds maximum size $options->{max_length}";
        if ( $options->{error_callback} ) {
            $options->{error_callback}->( $msg, $document, 'encode_one' );
        }
        else {
            Carp::croak("During encode_one, $msg");
        }
    }

    return $bson;
}

=method decode_one

    $doc = $codec->decode_one( $byte_string );
    $doc = $codec->decode_one( $byte_string, \%options );

Takes a byte string with a BSON-encoded document and returns a
hash reference representin the decoded document.

An optional hash reference of options may be provided.  Valid options include:

=for :list
* dbref_callback – overrides codec default
* dt_type – overrides codec default
* error_callback – overrides codec default
* max_length – overrides codec default

=cut

my @decode_overrides =
  qw/dbref_callback dt_type error_callback max_length/;
my $decode_one_args;

sub decode_one {
    $decode_one_args ||= compile( Object, Str, Optional [HashRef] );
    my ( $self, $string, $options ) = $decode_one_args->(@_);

    for my $k ( @decode_overrides ) {
        $options->{$k} = $self->$k unless exists $options->{$k};
    }

    if ( $options->{max_length} && length($string) > $options->{max_length} ) {
        my $msg = "Document exceeds maximum size $options->{max_length}";
        if ( $options->{error_callback} ) {
            $options->{error_callback}->( $msg, \$string, 'decode_one' );
        }
        else {
            Carp::croak("During decode_one, $msg");
        }
    }

    my $document = eval { MongoDB::BSON::_decode_bson( $string, $options ) };
    if ( $@ ) {
        if ( $options->{error_callback} ) {
            $options->{error_callback}->( $@, \$string, 'decode_one' );
        }
        else {
            Carp::croak("During decode_one, $@");
        }
    }

    return $document;
}

=method clone

    $codec->clone( dt_type => 'Time::Moment' );

Constructs a copy of the original codec, but allows changing
attributes in the copy.

=cut

sub clone {
    my ($self, @args) = @_;
    my $class = ref($self);
    if ( @args == 1 && ref( $args[0] ) eq 'HASH' ) {
        return $class->new( %$self, %{$args[0]} );
    }

    return $class->new( %$self, @args );
}


__PACKAGE__->meta->make_immutable;

1;

=head1 SYNOPSIS

    my $codec = MongoDB::BSON->new;

    my $bson = $codec->encode_one( $document );
    my $doc  = $codec->decode_one( $bson     );

=head1 DESCRIPTION

This class implements a BSON encoder/decoder ("codec").  It consumes documents
and emits BSON strings and vice versa.

=cut
