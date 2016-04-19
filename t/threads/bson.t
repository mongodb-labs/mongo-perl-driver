#
#  Copyright 2015
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

use strict;
use warnings;
use Config;
use if $Config{usethreads}, 'threads';
use Test::More;

BEGIN {
    plan skip_all => 'requires threads' unless $Config{usethreads};
    plan skip_all => 'needs Perl 5.10.1' unless $] ge '5.010001';
}

use MongoDB;
use Try::Tiny;
use threads::shared;

use lib "t/lib";

my $class = "BSON";

require_ok($class);

my $codec = $class->new;

my $var       = { a => "x" };
my $clone     = shared_clone $var;
my $enc_var   = $codec->encode_one($var);
my $enc_clone = $codec->encode_one($clone);

_bson_is( $enc_var, $enc_clone,
    "encoded top level hash and encoded top level shared hash" );
_bson_is(
    $codec->encode_one( { data => $var } ),
    $codec->encode_one( { data => $clone } ),
    "encoded hash and encoded shared hash"
);
_bson_is(
    $codec->encode_one( { data => $var->{a} } ),
    $codec->encode_one( { data => $clone->{a} } ),
    "encoded double and encoded shared clone of double"
);

threads->create(
    sub {
        _bson_is(
            $codec->encode_one($var),
            $codec->encode_one($clone),
            "(in thread) encoded top level hash and encoded top level shared hash"
        );
        _bson_is(
            $codec->encode_one( { data => $var } ),
            $codec->encode_one( { data => $clone } ),
            "(in thread) encoded hash and encoded shared hash"
        );
        _bson_is(
            $codec->encode_one( { data => $var->{a} } ),
            $codec->encode_one( { data => $clone->{a} } ),
            "(in thread) encoded double and encoded shared clone of double"
        );
    }
)->join;

sub _bson_is {
    my ( $got, $exp, $label ) = @_;
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    ok( $got eq $exp, $label )
      or diag "     Got:", _hexdump($got), "\nExpected:", _hexdump($exp), "\n";
}

sub _hexdump {
    my $str = shift;
    my $out = unpack("H*", $str);
    return $out;
}

done_testing();
