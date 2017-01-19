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

    # Test with and without asserts
    for my $n ( 0, 1 ) {
        print "Testing with PERL_MONGO_WITH_ASSERTS = $n\n";
        $ENV{PERL_MONGO_WITH_ASSERTS} = $n;
        make("test");
    }
};

