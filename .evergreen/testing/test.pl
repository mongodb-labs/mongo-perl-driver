#!/usr/bin/env perl
use strict;
use warnings;

use File::Spec;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Bootstrap
bootstrap_env();

# If MONGOD exists in the environment, we expect to be able to
# connect to it and want things to fail if we can't.  The
# EVG_ORCH_TEST environment variable stops tests from skipping
# without a mongod that tests can connect with.
$ENV{EVG_ORCH_TEST} = 1 if $ENV{MONGOD};

if ( $ENV{SSL} = 'ssl' ) {
    $ENV{EVG_TEST_SSL_PEM_FILE} = File::Spec->rel2abs("driver-tools/.evergreen/x509gen/client.pem");
    $ENV{EVG_TEST_SSL_CA_FILE}  = File::Spec->rel2abs("driver-tools/.evergreen/x509gen/ca.pem");
}

run_in_dir $ENV{REPO_DIR} => sub {
    # Configure ENV vars for local library updated in compile step
    bootstrap_locallib('local');

    # Configure & build (repeated to regenerate all object files)
    configure();
    make();

    # Run tests
    make("test");
};

