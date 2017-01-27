#!/usr/bin/env perl
use strict;
use warnings;

use File::Spec;
use File::Path 'rmtree';

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

my $tools_dir     = "driver-tools";
my $abs_tools_dir = File::Spec->rel2abs($tools_dir);
my $abs_orch_dir  = File::Spec->catdir( $abs_tools_dir, ".evergreen/orchestration" );
my $abs_mongodb_bin_dir = File::Spec->catdir( $abs_tools_dir, "mongodb/bin" );

# Download evergreen driver tool
rmtree("$tools_dir");
try_system( qw(git clone git://github.com/mongodb-labs/drivers-evergreen-tools.git),
    $tools_dir );

fix_shell_files_in($abs_tools_dir);
fix_config_files_in($abs_tools_dir);

# Add to the environment
$ENV{DRIVERS_TOOLS}            = $abs_tools_dir;
$ENV{MONGO_ORCHESTRATION_HOME} = $abs_orch_dir;
$ENV{MONGODB_BINARIES}         = $abs_mongodb_bin_dir;
$ENV{MONGODB_VERSION}          = $ENV{VERSION};

$ENV{MONGODB_VERSION} =~ s/^v//;
maybe_prepend_env( PATH => $abs_mongodb_bin_dir );

# Tell Mongo orchestration where to find binaries
spew( "$abs_orch_dir/orchestration.config", << "HERE");
{ "releases": { "default": "$abs_mongodb_bin_dir" } }
HERE

# Launch
try_system("sh $tools_dir/.evergreen/run-orchestration.sh");

exit 0;
