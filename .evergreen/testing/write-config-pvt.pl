#!/usr/bin/env perl
#
#  Copyright 2018 - present MongoDB, Inc.
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
use utf8;
use version;
use open qw/:std :utf8/;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenConfig;

# test: creates a test task; this extends the 'task' config helper to
# interpose extra steps before actually testing the driver

sub test {
    my %opts  = @_;
    my $name  = $opts{name} // 'unit_test';
    my $deps  = $opts{deps} // ['build'];
    my @extra = $opts{extra} ? @{ $opts{extra} } : ();
    return task(
        $name      => [ qw/whichPerl downloadBuildArtifacts/, @extra ],
        depends_on => $deps,
        filter     => $opts{filter},
    );
}

sub main {
    # Common tasks for all variants use this filter
    my $filter = { os => ['ubuntu1604'], perl => [qr/^24$/] };

    # repo_directory is replaced later from an Evergreen project variable.
    # It must go into the config.yml as '${repo_directory}'.  (I.e. this is
    # not a perl typo that fails to interpolate a variable.)
    my $download = [ 'downloadPerl5Lib' => { target => '${repo_directory}' } ];

    my @tasks = (
        pre( qw/dynamicVars cleanUp fetchSource/, $download ),
        post(qw/ cleanUp/),
        task( build => [qw/whichPerl buildModule uploadBuildArtifacts/], filter => $filter ),
        test(
            name   => "check",
            filter => $filter,
            deps   => ['build'],
            extra  => ['testDriver']
        ),
    );

    for my $uri_var (sort qw/replica sharded free tls11 tls12/) {
        push @tasks,
          test(
            name   => 'test_atlas_' . $uri_var,
            filter => $filter,
            deps   => ['check'],
            extra  => [ [ 'testLive' => { uri => sprintf( '${atlas_%s}', $uri_var ) } ] ],
          );
    }

    # Generate config
    print assemble_yaml(
        ignore( "/.evergreen/dependencies", "/.evergreen/toolchain" ),
        timeout(1800), buildvariants( \@tasks ),
    );

    return 0;
}

# execution
exit main();
