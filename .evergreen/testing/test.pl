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

# If testing under SSL, we set environment variables that our test helpers
# will use to set SSL connection parameters.
if ( $ENV{SSL} eq 'ssl' ) {
    if ( $ENV{ATLAS_PROXY} ) {
      $ENV{EVG_TEST_SSL_PEM_FILE} = "";
      $ENV{EVG_TEST_SSL_CA_FILE}  = File::Spec->rel2abs("atlasproxy/main/ca.pem");
    }
    else {
      $ENV{EVG_TEST_SSL_PEM_FILE} = File::Spec->rel2abs("driver-tools/.evergreen/x509gen/client.pem");
      $ENV{EVG_TEST_SSL_CA_FILE}  = File::Spec->rel2abs("driver-tools/.evergreen/x509gen/ca.pem");
    }
}

run_in_dir $ENV{REPO_DIR} => sub {
    # Configure ENV vars for local library updated in compile step
    bootstrap_locallib('local');

    # Configure & build (repeated to regenerate all object files)
    configure();
    make();

    # Enable fail point tests
    $ENV{FAILPOINT_TESTING} = 1;

    # Run tests with various combinations of environment config
    {
        local $ENV{PERL_MONGO_WITH_ASSERTS}=0;
        local $ENV{PERL_BSON_BACKEND}="BSON::PP";
        print "\n*** Testing with PERL_MONGO_WITH_ASSERTS=$ENV{PERL_MONGO_WITH_ASSERTS}\n";
        print "\n*** Testing with PERL_BSON_BACKEND=$ENV{PERL_BSON_BACKEND}\n";
        make("test");
    }
    {
        local $ENV{PERL_MONGO_WITH_ASSERTS}=1;
        local $ENV{PERL_BSON_BACKEND}="BSON::PP";
        print "\n*** Testing with PERL_MONGO_WITH_ASSERTS=$ENV{PERL_MONGO_WITH_ASSERTS}\n";
        print "\n*** Testing with PERL_BSON_BACKEND=$ENV{PERL_BSON_BACKEND}\n";
        make("test");
    }
    {
        local $ENV{PERL_MONGO_WITH_ASSERTS}=0;
        local $ENV{PERL_BSON_BACKEND}="";
        print "\n*** Testing with PERL_MONGO_WITH_ASSERTS=$ENV{PERL_MONGO_WITH_ASSERTS}\n";
        print "\n*** Testing with PERL_BSON_BACKEND=$ENV{PERL_BSON_BACKEND}\n";
        make("test");
    }
};

