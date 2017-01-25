#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenConfig;

sub main {
    my @tasks = (
        pre(qw/dynamicVars cleanUp fetchSource downloadPerl5Lib/),
        post(qw/cleanUp/),
        task( build => [qw/whichPerl buildModule uploadBuildArtifacts/] ),
        task(
            test       => [qw/whichPerl downloadBuildArtifacts testModule/],
            depends_on => 'build'
        ),
    );

    print assemble_yaml(
        ignore( "/.evergreen/dependencies", "/.evergreen/toolchain" ),
        timeout(1800), buildvariants( \@tasks ),
    );

    return 0;
}

# execution
exit main();
