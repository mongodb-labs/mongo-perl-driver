#!/usr/bin/env perl
use strict;
use warnings;

use File::Spec;
use File::Path 'rmtree';

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Constants
my $tools_dir     = "driver-tools";
my $abs_tools_dir = File::Spec->rel2abs($tools_dir);
my $abs_orch_dir  = File::Spec->catdir( $abs_tools_dir, ".evergreen/orchestration" );
my $abs_mongodb_bin_dir = File::Spec->catdir( $abs_tools_dir, "mongod/bin" );
my $stop_script = "$tools_dir/.evergreen/stop-orchestration.sh";

# If no evidence of mongo-orchestration, short circuit
exit 0 unless -d $tools_dir;

# Add to the environment
$ENV{DRIVERS_TOOLS}            = $abs_tools_dir;
$ENV{MONGO_ORCHESTRATION_HOME} = $abs_orch_dir;
$ENV{MONGODB_BINARIES}         = $abs_mongodb_bin_dir;
maybe_prepend_env( PATH => $abs_mongodb_bin_dir );

# If the PR to add a stop script isn't merged yet, add it
if ( !-f $stop_script ) {
    spew( $stop_script, << 'HERE');
#!/bin/sh
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

cd "$MONGO_ORCHESTRATION_HOME"
# source the mongo-orchestration virtualenv if it exists
if [ -f venv/bin/activate ]; then
    . venv/bin/activate
elif [ -f venv/Scripts/activate ]; then
    . venv/Scripts/activate
fi
mongo-orchestration stop
HERE
}

# Launch
eval { try_system("sh $tools_dir/.evergreen/stop-orchestration.sh"); }
warn $@ if $@;

rmtree($abs_tools_dir);

exit 0;
