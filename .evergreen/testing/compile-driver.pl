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
    # Install any new, missing dependencies to local library
    run_local_cpanm(qw/--installdeps ./);

    # Configure & build
    configure();
    make();

    # Archive both built blib and local deps for reuse
    try_system(qw/tar -czf build.tar.gz blib local/);
};
