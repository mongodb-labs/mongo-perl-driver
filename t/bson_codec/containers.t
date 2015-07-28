#
#  Copyright 2015 MongoDB, Inc.
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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep;
use Tie::IxHash;

use MongoDB;
use MongoDB::OID;

use lib 't/lib';
use TestBSON;

my $oid = MongoDB::OID->new("554ce5e4096df3be01323321");
my $bin_oid = pack( "C*", map hex($_), unpack( "(a2)12", "$oid" ) );

my $class = "MongoDB::BSON";

require_ok($class);

my $codec = new_ok( $class, [], "new with no args" );

my @cases = (
    {
        label => "empty doc",
        opts  => {},
        input => [],
        bson  => _doc(""),
    },
    {
        label => "BSON double",
        opts  => {},
        input => [ a => 1.23 ],
        bson  => _doc( BSON_DOUBLE . _ename("a") . _double(1.23) ),
    },
    {
        label => "BSON string",
        opts  => {},
        input => [ a => 'b' ],
        bson  => _doc( BSON_STRING . _ename("a") . _string("b") ),
    },
    {
        label => "BSON OID",
        opts  => {},
        input => [ _id => $oid ],
        bson  => _doc( BSON_OID . _ename("_id") . $bin_oid ),
    },
    {
        label => "add _id",
        opts  => {
            first_key   => '_id',
            first_value => $oid,
        },
        input => [],
        bson  => _doc( BSON_OID . _ename("_id") . $bin_oid ),
    },
    {
        label => "add _id, ignore existing",
        opts  => {
            first_key   => '_id',
            first_value => $oid,
        },
        input => [ _id => "12345" ],
        bson  => _doc( BSON_OID . _ename("_id") . $bin_oid ),
    },
    {
        label => "add _id with null",
        opts  => { first_key => '_id', },
        input => [ _id => "12345" ],
        bson  => _doc( BSON_NULL . _ename("_id") ),
    },
    {
        label => "empty key is error",
        opts  => {},
        input => [ "" => "12345" ],
        error => qr/empty key name/,
    },
    {
        label => "dot in key is normally valid",
        opts  => {},
        input => [ "a.b" => "c" ],
        bson  => _doc( BSON_STRING . _ename("a.b") . _string("c") ),
    },
    {
        label => "dot in key fails invalid check",
        opts  => { invalid_chars => '.' },
        input => [ "a.b" => "c" ],
        error => qr/cannot contain the '\.' character/,
    },
    {
        label => "dot in key fails multi invalid chars",
        opts  => { invalid_chars => '_$' },
        input => [ '$ab' => "c" ],
        error => qr/cannot contain the '\$' character/,
    },
    {
        label => "op_char replacement",
        opts  => { op_char => '-' },
        input => [ '-a' => "c" ],
        bson  => _doc( BSON_STRING . _ename('$a') . _string("c") ),
    },
    {
        label => "op_char change before invalid check",
        opts  => { op_char => '.', invalid_chars => '.' },
        input => [ '.a' => "c" ],
        bson  => _doc( BSON_STRING . _ename('$a') . _string("c") ),
    },
    {
        label => "op_char and invalid check ignore empty string",
        opts  => { op_char => '', invalid_chars => '' },
        input => [ '.a' => "c" ],
        bson  => _doc( BSON_STRING . _ename('.a') . _string("c") ),
    },
    {
        label => "prefer_numeric false",
        opts  => {},
        input => [ a => "1.23" ],
        bson  => _doc( BSON_STRING . _ename("a") . _string("1.23") ),
    },
    {
        label => "prefer_numeric true",
        opts  => { prefer_numeric => 1 },
        input => [ a => "1.23" ],
        bson  => _doc( BSON_DOUBLE . _ename("a") . _double(1.23) ),
    },
    {
        label => "BSON too long",
        opts  => { max_length => 2 },
        input => [ 'a' => 'b' ],
        error => qr/exceeds maximum size 2/,
    },
    {
        label => "BSON too long",
        opts  => {
            invalid_chars  => '.',
            error_callback => sub { die "Bad $_[1]: $_[0]" },
        },
        input => [ 'a.b' => 'b' ],
        error => qr/Bad (?:[A-Za-z:]+=)?\w+\(0x[a-f0-9]+\):.*the '\.' character/,
    },
);

for my $c (@cases) {
    if ( $c->{bson} ) {
        valid_case($c);
    }
    elsif ( $c->{error} ) {
        error_case($c);
    }
    else {
        die "Unknown case type for '$c->{label}'";
    }
}

# have to check one-off as we won't get this via a round-trip
{
    my $bson  = _doc( BSON_STRING . _ename("a") . _string("a"x20) );
    like(
        exception { $codec->decode_one( $bson, { max_length => 5 } ) },
        qr/exceeds maximum size 5/,
        "decode exceeding max_length throws error"
    );
}

# array documents can't have duplicate keys
{
    like(
        exception { $codec->encode_one( [ x => 1, y => 2, z => 3, y => 4 ] ) },
        qr/duplicate key 'y'/,
        "duplicate key in array document is fatal"
    );
}

#--------------------------------------------------------------------------#
# support functions
#--------------------------------------------------------------------------#

sub valid_case {
    my $c = shift;
    my ( $label, $input, $bson, $opts ) = @{$c}{qw/label input bson opts/};
    my ( $doc, $got );
    subtest $label => sub {
        # hash style
        $doc = {@$input};
        $got = $codec->encode_one( $doc, $opts );
        is_bin( $got, $bson, "encode_one( HASH )" );
        cmp_deeply( $doc, {@$input}, "doc unmodified" );

        # array style
        $doc = [@$input];
        $got = $codec->encode_one( $doc, $opts );
        is_bin( $got, $bson, "encode_one( ARRAY )" );
        cmp_deeply( $doc, [@$input], "doc unmodified" );

        # IxHash
        $doc = Tie::IxHash->new(@$input);
        $got = $codec->encode_one( $doc, $opts );
        is_bin( $got, $bson, "encode_one( IxHash )" );
        cmp_deeply( $doc, Tie::IxHash->new(@$input), "doc unmodified" );
    };

}

sub error_case {
    my $c = shift;
    my ( $label, $input, $error, $opts ) = @{$c}{qw/label input error opts/};
    my ( $doc, $got );
    subtest $label => sub {
        # hash style
        $doc = {@$input};
        like( exception { $got = $codec->encode_one( $doc, $opts ) },
            $error, "exception for HASH" );

        # array style
        $doc = [@$input];
        like( exception { $got = $codec->encode_one( $doc, $opts ) },
            $error, "exception for ARRAY" );

        # IxHash
        $doc = Tie::IxHash->new(@$input);
        like( exception { $got = $codec->encode_one( $doc, $opts ) },
            $error, "exception for Tie::IxHash" );
    };
}

done_testing;

# vim: ts=4 sts=4 sw=4 et:
