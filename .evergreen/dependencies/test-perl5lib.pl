#!/usr/bin/env perl
use strict;
use warnings;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Bootstrap
bootstrap_env();

# Try loading a couple required modules
require Config::AutoConf;
require Moo;

exit 0;
