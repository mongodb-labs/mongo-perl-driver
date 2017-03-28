#!/usr/bin/env perl
use strict;
use warnings;

# This file does preparatory work for launching the driver-evergreen-tools
# script to start mongo-orchestration and then actually launches it.

use File::Copy 'cp';
use File::Spec;
use File::Path 'rmtree';

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Constants normalized to unix-style (even on Windows)
my $tools_dir     = "drivers-tools";
my $abs_tools_dir = fwd_slash( File::Spec->rel2abs($tools_dir) );
my $abs_mongodb_bin_dir =
  fwd_slash( File::Spec->catdir( $abs_tools_dir, "mongodb/bin" ) );

# Download evergreen driver tool
rmtree("$tools_dir");
try_system(
    qw(git clone https://github.com/mongodb-labs/drivers-evergreen-tools.git),
    $tools_dir );

fix_shell_files_in($abs_tools_dir);

# Add to the environment
$ENV{DRIVERS_TOOLS} = $abs_tools_dir;
maybe_prepend_env( PATH => $abs_mongodb_bin_dir );

# Launch
try_system("sh $tools_dir/.evergreen/run-atlas-proxy.sh");

exit 0;
