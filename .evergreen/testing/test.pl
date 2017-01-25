#!/usr/bin/env perl
use strict;
use warnings;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Bootstrap
bootstrap_env();

run_in_dir $ENV{REPO_DIR} => sub {
    # Configure ENV vars for local library updated in compile step
    bootstrap_locallib('local');

    # Configure & build (repeated to regenerate all object files)
    configure();
    make();

    # Run tests
    make("test");
};

