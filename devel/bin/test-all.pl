#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use IPC::Cmd;

die "Usage: $0 <program> [args...]\n"
  unless @ARGV;

die "$ARGV[0] not found or not executable\n"
  unless IPC::Cmd::can_run( $ARGV[0] );

for my $file (<devel/clusters/*.yml>) {
    say "---- TESTING WITH $file ----";
    system( "devel/bin/harness.pl", $file, @ARGV );
}
