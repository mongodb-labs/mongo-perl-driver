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

use lib 't/lib';
use TestBSON;

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
    {
        label  => "BSON Int32",
        input  => { a => 66 },
        bson   => _doc( BSON_INT32 . _ename("a") . _int32(66) ),
        output => { a => 66 },
    },
    {
        label  => "BSON Int32 (max 32 bit int)",
        input  => { a => MAX_LONG },
        bson   => _doc( BSON_INT32 . _ename("a") . _int32(MAX_LONG) ),
        output => { a => MAX_LONG },
    },
    {
        label  => "BSON Int32 (min 32 bit int)",
        input  => { a => MIN_LONG },
        bson   => _doc( BSON_INT32 . _ename("a") . _int32(MIN_LONG) ),
        output => { a => MIN_LONG },
    },
);

if (HAS_INT64) {
    my $big = 20 << 40;
    push @cases,
      {
        label  => "BSON Int64",
        input  => { a => $big },
        bson   => _doc( BSON_INT64 . _ename("a") . _int64($big) ),
        output => { a => $big },
      },
    {
        label  => "BSON Int64 (small bigint)",
        input  => { a => Math::BigInt->new(66) },
        bson   => _doc( BSON_INT64 . _ename("a") . _int64(66) ),
        output => { a => 66 },
    },
      {
        label  => "BSON Int64 (64 bit pos igint)",
        input  => { a => Math::BigInt->new( MAX_LONG + 1 ) },
        bson   => _doc( BSON_INT64 . _ename("a") . _int64( MAX_LONG + 1 ) ),
        output => { a => MAX_LONG + 1 },
      },
      {
        label  => "BSON Int64 (64 bit neg bigint)",
        input  => { a => Math::BigInt->new( MIN_LONG - 1 ) },
        bson   => _doc( BSON_INT64 . _ename("a") . _int64( MIN_LONG - 1 ) ),
        output => { a => MIN_LONG - 1 },
      };
}
else {
    my $neg_sml = Math::BigInt->new( -66 );
    my $pos_sml = Math::BigInt->new( 66 );
    my $neg_big = Math::BigInt->new( MIN_LONG )->bsub ( 1 );
    my $pos_big = Math::BigInt->new( MAX_LONG )->badd ( 1 );
    push @cases,
    {
        label  => "BSON Int64 (small pos bigint)",
        input  => { a => $pos_sml },
        bson   => _doc( BSON_INT64 . _ename("a") . _int64(66) ),
        output => { a => $pos_sml },
    },
    {
        label  => "BSON Int64 (small neg bigint)",
        input  => { a => $neg_sml },
        bson   => _doc( BSON_INT64 . _ename("a") . _int64(-66) ),
        output => { a => $neg_sml },
    },
    {
	label  => "BSON Int64 (64 bit pos bigint)",
	input  => { a => $pos_big },
	bson   => _doc( BSON_INT64 . _ename("a") . _int64( $pos_big ) ),
	output => { a => $pos_big },
    },
    {
	label  => "BSON Int64 (64 bit neg bigint)",
	input  => { a => $neg_big },
	bson   => _doc( BSON_INT64 . _ename("a") . _int64( $neg_big ) ),
	output => { a => $neg_big },
    };
}

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

subtest "bigint over/underflow" => sub {
    # these are greater/less than LLONG_MAX/MIN
    my $too_big   = Math::BigInt->new("9223372036854775808");
    my $too_small = Math::BigInt->new("-9223372036854775809");

    for my $data ( $too_big, $too_small ) {
        like(
            exception { $codec->encode_one( { a => $data } ) },
            qr/Math::BigInt '-?\d+' can't fit into a 64-bit integer/,
            "error encoding $data"
        );
    }
};

done_testing;

# vim: ts=4 sts=4 sw=4 et:
