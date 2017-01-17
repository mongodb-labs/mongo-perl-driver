#!/usr/bin/env perl
use strict;
use warnings;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Bootstrap
bootstrap_env();

run_in_dir 'mongo-perl-driver' => sub {
    # install any new, missing dependencies to local library
    run_local_cpanm("--installdeps .");

    # Configure, build
    try_system("perl Makefile.PL");
    try_system("make");
    try_system("tar -czf build.tar.gz blib local");
};
