#!/usr/bin/env perl
#
#  Copyright 2014 - present MongoDB, Inc.
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

use v5.10;
use strict;
use warnings;

use IPC::Cmd;

die "Usage: $0 <program> [args...]\n"
  unless @ARGV;

die "$ARGV[0] not found or not executable\n"
  unless IPC::Cmd::can_run( $ARGV[0] );

my @versions = qw(2.4 2.6 3.0 3.2 any);

my @types = qw(mongod replicaset sharded);

for my $t ( @types ) {
    for my $v ( @versions ) {
        my $file = "devel/config/${t}-${v}.yml";
        next unless -f $file;
        say "---- TESTING WITH $file ----";
        if ( -f $file ) {
            system( "devel/bin/harness.pl", "-v", $file, @ARGV );
            say "---- RESULT WITH $file: " . ($? ? "FAIL" : "PASS") . " ----";
        }
    }
}
