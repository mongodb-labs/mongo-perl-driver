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

use strict;
use warnings;
use Config;
use if $Config{usethreads}, 'threads';
use Test::More;

BEGIN { plan skip_all => 'requires threads' unless $Config{usethreads} }

BEGIN { plan skip_all => 'threads not supported before Perl 5.8.5' unless $] ge "5.008005" }

use MongoDB;
use BSON::OID;

my @threads = map {
    threads->create(sub {
        [map { BSON::OID->new } 0 .. 3]
    });
} 0 .. 9;

my @oids = map { @{ $_->join } } @threads;

my @inc = sort { $a <=> $b }  map {
    unpack 'v', (pack('H*', $_) . '\0')
} map { substr $_->value, 20 } @oids;

my $prev = -1;
for (@inc) {
    ok($prev < $_);
    $prev = $_;
}

done_testing();
