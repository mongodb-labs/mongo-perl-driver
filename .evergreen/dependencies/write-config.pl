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
        pre(qw/dynamicVars cleanUp cleanUpOtherRepos fetchSource fetchOtherRepos/),
        post(qw/cleanUp cleanUpOtherRepos/),
    );

    for my $dir (qw/mongo-perl-driver mongo-perl-bson mongo-perl-bson-xs/) {
        ( my $name = $dir ) =~ s/mongo-perl-//;
        $name =~ tr[-][_];
        my $vars = { target => $dir };

        my $build    = [ 'buildPerl5Lib'    => $vars ];
        my $upload   = [ 'uploadPerl5Lib'   => $vars ];
        my $download = [ 'downloadPerl5Lib' => $vars ];

        push @tasks, task( "build_${name}_perl5lib" => [ 'whichPerl', $build, $upload ] );

        push @tasks,
          task(
            "test_${name}_perl5lib" => [ 'whichPerl', $download, 'testPerl5Lib' ],
            depends_on              => "build_${name}_perl5lib"
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