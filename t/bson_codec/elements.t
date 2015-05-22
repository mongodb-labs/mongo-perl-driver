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
use Test::Deep 0.086; # num() function
use Test::Fatal;

use Config;
use DateTime;
use DateTime::Tiny;
use Math::BigInt;
use MongoDB;
use MongoDB::OID;
use MongoDB::DBRef;

my $oid = MongoDB::OID->new("554ce5e4096df3be01323321");
my $bin_oid = pack( "C*", map hex($_), unpack( "(a2)12", "$oid" ) );

my $regexp = MongoDB::BSON::Regexp->new( pattern => "abcd", flags => "ismx" );

my $dt = DateTime->new(
    year       => 1984,
    month      => 10,
    day        => 16,
    hour       => 16,
    minute     => 12,
    second     => 47,
    nanosecond => 500_000_000,
    time_zone  => 'UTC',
);
my $dt_epoch_fraction = $dt->epoch + $dt->nanosecond / 1e9;

my $dtt = DateTime::Tiny->new(
    year   => 1984,
    month  => 10,
    day    => 16,
    hour   => 16,
    minute => 12,
    second => 47,
);

my $dbref = MongoDB::DBRef->new( db => 'test', ref => 'test_coll', id => '123' );
my $dbref_cb = sub {
    my $hr = shift;
    return [ map { $_ => $hr->{$_} } sort keys %$hr ];
};

use constant {
    PERL58   => $] lt '5.010',
    HASINT64 => $Config{use64bitint}
};

use constant {
    P_INT32 => PERL58 ? "l" : "l<",
    P_INT64 => PERL58 ? "q" : "q<",
    BSON_DOUBLE   => "\x01",
    BSON_STRING   => "\x02",
    BSON_DOC      => "\x03",
    BSON_OID      => "\x07",
    BSON_DATETIME => "\x09",
    BSON_NULL     => "\x0A",
    BSON_REGEXP   => "\x0B",
};

my $class = "MongoDB::BSON";

require_ok($class);

my $codec = new_ok( $class, [], "new with no args" );

my @cases = (
    {
        label  => "BSON double",
        input  => { a => 1.23 },
        bson   => _doc( BSON_DOUBLE . _ename("a") . _double(1.23) ),
        output => { a => num( 1.23, 1e-6 ) },
    },
    {
        label  => "BSON string",
        input  => { a => 'b' },
        bson   => _doc( BSON_STRING . _ename("a") . _string("b") ),
        output => { a => 'b' },
    },
    {
        label  => "BSON OID",
        input  => { _id => $oid },
        bson   => _doc( BSON_OID . _ename("_id") . $bin_oid ),
        output => { _id => $oid },
    },
    {
        label  => "BSON Regexp (qr to obj)",
        input  => { re => qr/abcd/imsx },
        bson   => _doc( BSON_REGEXP . _ename("re") . _regexp( 'abcd', 'imsx' ) ),
        output => { re => $regexp },
    },
    {
        label  => "BSON Regexp (obj to obj)",
        input  => { re => $regexp },
        bson   => _doc( BSON_REGEXP . _ename("re") . _regexp( 'abcd', 'imsx' ) ),
        output => { re => $regexp },
    },
    {
        label    => "BSON Datetime from DateTime to raw",
        input    => { a => $dt },
        bson     => _doc( BSON_DATETIME . _ename("a") . _datetime($dt) ),
        dec_opts => { dt_type => undef },
        output   => { a => $dt->epoch },
    },
    {
        label    => "BSON Datetime from DateTime::Tiny to DateTime::Tiny",
        input    => { a => $dtt },
        bson     => _doc( BSON_DATETIME . _ename("a") . _datetime( $dtt->DateTime ) ),
        dec_opts => { dt_type => "DateTime::Tiny" },
        output   => { a => $dtt },
    },
    {
        label    => "BSON Datetime from DateTime to DateTime",
        input    => { a => $dt },
        bson     => _doc( BSON_DATETIME . _ename("a") . _datetime($dt) ),
        dec_opts => { dt_type => "DateTime" },
        output   => { a => DateTime->from_epoch( epoch => $dt_epoch_fraction ) },
    },
    {
        label => "BSON DBRef to unblessed",
        input => { a => $dbref },
        bson  => _doc( BSON_DOC . _ename("a") . _dbref($dbref) ),
        output =>
          { a => { '$ref' => $dbref->ref, '$id' => $dbref->id, '$db' => $dbref->db } },
    },
    {
        label    => "BSON DBRef to arrayref",
        input    => { a => $dbref },
        bson     => _doc( BSON_DOC . _ename("a") . _dbref($dbref) ),
        dec_opts => { dbref_callback => $dbref_cb },
        output =>
          { a => [ '$db' => $dbref->db, '$id' => $dbref->id, '$ref' => $dbref->ref ] },
    },
);

for my $c (@cases) {
    my ( $label, $input, $bson, $output ) = @{$c}{qw/label input bson output/};
    my $encoded = $codec->encode_one( $input, $c->{enc_opts} || {} );
    is_bin( $encoded, $bson, "$label: encode_one" );
    if ($output) {
        my $decoded = $codec->decode_one( $encoded, $c->{dec_opts} || {} );
        cmp_deeply( $decoded, $output, "$label: decode_one" )
          or diag "GOT:", explain($decoded), "EXPECTED:", explain($output);
    }
}

sub is_bin {
    my ( $got, $exp, $label ) = @_;
    $label ||= '';
    s{([^[:graph:]])}{sprintf("\\x{%02x}",ord($1))}ge for $got, $exp;
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    is( $got, $exp, $label );
}

sub _doc {
    my ($string) = shift;
    return pack( P_INT32, 5 + length($string) ) . $string . "\x00";
}

sub _cstring { return $_[0] . "\x00" }
BEGIN { *_ename = \&_cstring }

sub _double { return pack( "d", shift ) }

sub _string {
    my ($string) = shift;
    return pack( P_INT32, 1 + length($string) ) . $string . "\x00";
}

sub _datetime {
    my $dt = shift;
    if (HASINT64) {
        return pack( P_INT64, 1000 * $dt->epoch + $dt->millisecond );
    }
    else {
        my $big = Math::BigInt->new( $dt->epoch );
        $big->bmul(1000);
        $big->badd( $dt->millisecond );
        return _pack_bigint($big);
    }
}

sub _regexp {
    my ( $pattern, $flags ) = @_;
    return _cstring($pattern) . _cstring($flags);
}

sub _dbref {
    my $dbref = shift;
    #<<< No perltidy
    return _doc(
          BSON_STRING . _ename('$ref') . _string($dbref->ref)
        . BSON_STRING . _ename('$id' ) . _string($dbref->id)
        . BSON_STRING . _ename('$db' ) . _string($dbref->db)
    );
    #>>>
}

# pack to int64_t
sub _pack_bigint {
    my $big    = shift;
    my $as_hex = $big->as_hex; # big-endian hex
    substr( $as_hex, 0, 2, '' ); # remove "0x"
    my $len = length($as_hex);
    substr( $as_hex, 0, 0, "0" x ( 16 - $len ) ) if $len < 16; # pad to quad length
    my $packed = pack( "H*", $as_hex );                        # packed big-endian
    return reverse($packed);                                   # reverse to little-endian
}

done_testing;

# vim: ts=4 sts=4 sw=4 et:
