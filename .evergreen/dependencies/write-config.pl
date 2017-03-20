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

    # Define pre and post tasks

    my @tasks = (
        pre(qw/dynamicVars cleanUp cleanUpOtherRepos fetchSource fetchOtherRepos/),
        post(qw/cleanUp cleanUpOtherRepos/),
    );

    # We build dependency local-libs separately for each project.  This
    # loop adds repo-specific tasks.
    #
    # We set stepback to false: if deps don't build, it's probably
    # transient due to upstream errors and there's no reason to walk
    # back through commit histories hoping it will pass.

    for my $dir (qw/mongo-perl-driver mongo-perl-bson mongo-perl-bson-xs/) {
        ( my $name = $dir ) =~ s/mongo-perl-//;
        $name =~ tr[-][_];
        my $vars = { target => $dir };

        my $build    = [ 'buildPerl5Lib'    => $vars ];
        my $upload   = [ 'uploadPerl5Lib'   => $vars ];
        my $download = [ 'downloadPerl5Lib' => $vars ];

        push @tasks,
          task(
            "build_${name}_perl5lib" => [ 'whichPerl', $build, $upload ],
            stepback                 => 'false'
          );

        push @tasks,
          task(
            "test_${name}_perl5lib" => [ 'whichPerl', $download, 'testPerl5Lib' ],
            depends_on              => "build_${name}_perl5lib",
            stepback                => 'false',
          );
    }

    print assemble_yaml(
        # Ignore everything except changes to the dependencies files
        ignore( "*", "!/.evergreen/dependencies/*", "!/.evergreen/lib/*" ),
        timeout(3600),
        buildvariants( \@tasks ),
    );

    return 0;
}

# execution
exit main();
