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
use Math::BigInt;
use MongoDB;

use lib "t/lib";
use TestBSON;

plan skip_all => "Requires Time::Moment"
  unless eval { require Time::Moment; 1 };

require DateTime;

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

my $tm                = Time::Moment->from_object($dt);
my $dt_epoch_fraction = $dt->epoch + $dt->nanosecond / 1e9;

my $class = "MongoDB::BSON";

require_ok($class);

my $codec = new_ok( $class, [], "new with no args" );

my @cases = (
    {
        label    => "BSON Datetime from DateTime to Time::Moment",
        input    => { a => $dt },
        bson     => _doc( BSON_DATETIME . _ename("a") . _datetime($dt) ),
        dec_opts => { dt_type => "Time::Moment" },
        output   => { a => $tm },
    },
    {
        label    => "BSON Datetime from Time::Moment to Time::Moment",
        input    => { a => $tm },
        bson     => _doc( BSON_DATETIME . _ename("a") . _datetime($dt) ),
        dec_opts => { dt_type => "Time::Moment" },
        output   => { a => $tm },
    },
);

for my $c (@cases) {
    my ( $label, $input, $bson, $output ) = @{$c}{qw/label input bson output/};
    my $encoded = $codec->encode_one( $input, $c->{enc_opts} || {} );
    is_bin( $encoded, $bson, "$label: encode_one" );
    if ($output) {
        my $decoded = $codec->decode_one( $encoded, $c->{dec_opts} || {} );
        cmp_deeply( $decoded, $output, "$label: decode_one" )
          or diag "GOT:", _hexdump( explain($decoded) ), "EXPECTED:",
          _hexdump( explain($output) );
    }
}

done_testing;

# vim: ts=4 sts=4 sw=4 et:
