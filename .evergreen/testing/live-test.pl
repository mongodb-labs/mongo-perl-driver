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

use strict;
use warnings;

use File::Spec;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Bootstrap
bootstrap_env();

run_in_dir $ENV{REPO_DIR} => sub {
    # Configure ENV vars for local library updated in compile step
    bootstrap_locallib('local');

    # Configure & build (repeated to regenerate all object files)
    configure();
    make();

    # Run tests
    make("test", "TEST_FILES=devel/t-special/PERL-834-atlas.t");
};

