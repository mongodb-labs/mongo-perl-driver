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

use MongoDB;
use boolean;

use lib "t/lib";
use TestBSON;

my $class = "MongoDB::BSON";

require_ok($class);

my $codec = new_ok( $class, [], "new with no args" );

my @cases = qw(
  boolean
  JSON::PP
  Types::Serialiser
  Cpanel::JSON::XS
  Mojo::JSON
  JSON::Tiny
);

for my $c (@cases) {
    subtest "class $c" => sub {
        plan skip_all => "requires $c"
          unless eval "require $c; 1";

        my $input = [
            true  => eval "${c}::true()",
            false => eval "${c}::false()",
        ];

        my $bson =
          _doc( BSON_BOOL . _ename("true") . "\x01" . BSON_BOOL . _ename("false") . "\x00" );

        my $output = {
            true  => boolean::true,
            false => boolean::false,
        };

        my $encoded = $codec->encode_one( $input, {} );
        is_bin( $encoded, $bson, "encode_one" );
        my $decoded = $codec->decode_one( $encoded, {} );
        cmp_deeply( $decoded, $output, "decode_one" )
          or diag "GOT:", _hexdump( explain($decoded) ), "EXPECTED:",
          _hexdump( explain($output) );
      }
}

done_testing;

# vim: ts=4 sts=4 sw=4 et:
