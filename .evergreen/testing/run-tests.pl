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
    # Configure ENV vars for local library updated in compile step
    bootstrap_locallib('local');

    # Test without asserts
    print "Testing with PERL_MONGO_WITH_ASSERTS = 0\n";
    $ENV{PERL_MONGO_WITH_ASSERTS} = 0;
    try_system("make test");

    # Test with asserts
    print "Testing with PERL_MONGO_WITH_ASSERTS = 1\n";
    $ENV{PERL_MONGO_WITH_ASSERTS} = 1;
    try_system("make test");
};

