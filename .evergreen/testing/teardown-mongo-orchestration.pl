#!/usr/bin/env perl
#
#  Copyright 2017 - present MongoDB, Inc.
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
