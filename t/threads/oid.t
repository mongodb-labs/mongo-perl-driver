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
} map { substr $_, 20 } @oids;

my $prev = -1;
for (@inc) {
    ok($prev < $_);
    $prev = $_;
}

done_testing;
