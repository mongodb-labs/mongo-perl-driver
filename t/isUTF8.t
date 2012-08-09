#!/usr/bin/env perl

use Test::More;
use MongoDB;


my %valid = (
    "abc123"           => 1,
    "\xc0\x81"         => 0,
    "\xc1\xa0"         => 0,
    "\xc2\x81"         => 1,
    "\xdf\x80"         => 1,
    "\xdf\xc0"         => 0,
    "\xe0\x80"         => 0,
    "\xe0\x81\x80"     => 0,
    "\xe0\xa0\x80"     => 1,
    "\xed\xa0\x80"     => 0,
    "\xee\x81\x81"     => 1,
    "\xe9a"            => 0,
    "\xf0\x90\xbe\xbf" => 1,
    "\xf2\x79\x80\x80" => 0,
    "\xf4\x8f\xbf\x80" => 1,
);


for my $k (sort keys %valid) {
    my $bytes  = pack('C*', unpack('C*', $k));
    my $isUTF8 = MongoDB::_test_is_utf8($bytes);
    my $hex    = join ' ', map { sprintf( "%x", ord($_) ) } (split(//,$bytes));
    my $vStr   = ($valid{$k}) ? 'valid' : 'invalid'; 
    is( $isUTF8, $valid{$k}, "utf8-test: $hex ($vStr)" );
}

done_testing();
