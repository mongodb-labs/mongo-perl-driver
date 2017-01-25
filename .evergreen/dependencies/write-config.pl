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
        pre( qw/dynamicVars cleanUp cleanUpOtherRepos fetchSource fetchOtherRepos/ ),
        post( qw/cleanUp cleanUpOtherRepos/ ),
        task( build_perl5lib => [qw/whichPerl buildPerl5Lib uploadPerl5Lib/] ),
        task(
            test_perl5lib => [qw/whichPerl downloadPerl5Lib testLoadPerlDriver/],
            depends_on      => 'build_perl5lib'
        ),
    );

    print assemble_yaml(
        # Ignore everything except changes to the dependencies files
        ignore( "*", "!/.evergreen/dependencies/*", "!/.evergreen/lib/*" ),
        timeout(3600),
        buildvariants(\@tasks),
    );

    return 0;
}

# execution
exit main();
