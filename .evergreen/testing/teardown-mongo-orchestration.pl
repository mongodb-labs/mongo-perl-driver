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
my $abs_tools_dir = fwd_slash( File::Spec->rel2abs($tools_dir) );
my $abs_orch_dir =
  fwd_slash( File::Spec->catdir( $abs_tools_dir, ".evergreen/orchestration" ) );
my $abs_mongodb_bin_dir =
  fwd_slash( File::Spec->catdir( $abs_tools_dir, "mongod/bin" ) );
my $stop_script = "$tools_dir/.evergreen/stop-orchestration.sh";

# If no evidence of mongo-orchestration, short circuit
exit 0 unless -d $tools_dir;

# Add to the environment
$ENV{DRIVERS_TOOLS}            = $abs_tools_dir;
$ENV{MONGO_ORCHESTRATION_HOME} = $abs_orch_dir;
$ENV{MONGODB_BINARIES}         = $abs_mongodb_bin_dir;
maybe_prepend_env( PATH => $abs_mongodb_bin_dir );

# Launch
eval { try_system("sh $stop_script"); };
warn $@ if $@;

rmtree($abs_tools_dir);

exit 0;
