#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;

use IPC::Cmd;

die "Usage: $0 <program> [args...]\n"
  unless @ARGV;

die "$ARGV[0] not found or not executable\n"
  unless IPC::Cmd::can_run( $ARGV[0] );

my @versions = qw(2.0 2.2 2.4 2.6 any);

my @types = qw(mongod master replicaset sharded);

for my $t ( @types ) {
    for my $v ( @versions ) {
        my $file = "devel/config/${t}-${v}.yml";
        say "---- TESTING WITH $file ----";
        if ( -f $file ) {
            system( "devel/bin/harness.pl", $file, @ARGV );
        }
        else {
            say "FILE: $file not found";
        }
    }
}
