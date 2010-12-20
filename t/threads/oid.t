use strict;
use warnings;
use Test::More;

use MongoDB;
use MongoDB::OID;

use threads;

my @threads = map {
    threads->create(sub {
        [map { MongoDB::OID->build_value } 0 .. 3]
    });
} 0 .. 9;

my @oids = map { @{ $_->join } } @threads;

my @inc = sort { $a <=> $b }  map {
    unpack 'v', (pack('H*', $_) . '\0')
} map { substr $_, 18 } @oids;

is_deeply [@inc], [0 .. 39], 'strictly ascending inc parts in OIDs';

done_testing;
