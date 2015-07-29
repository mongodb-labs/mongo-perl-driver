#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use IPC::Cmd;

die "Usage: $0 <program> [args...]\n"
  unless @ARGV;

die "$ARGV[0] not found or not executable\n"
  unless IPC::Cmd::can_run( $ARGV[0] );

my @versions = qw(2.4 2.6 3.0 any);

my @types = qw(mongod replicaset sharded);

for my $t ( @types ) {
    for my $v ( @versions ) {
        my $file = "devel/config/${t}-${v}.yml";
        next unless -f $file;
        say "---- TESTING WITH $file ----";
        if ( -f $file ) {
            system( "devel/bin/harness.pl", $file, @ARGV );
            say "---- RESULT WITH $file: " . ($? ? "FAIL" : "PASS") . " ----";
        }
    }
}
