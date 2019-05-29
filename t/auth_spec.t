#  Copyright 2016 - present MongoDB, Inc.
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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::Fatal;
use Test::More;

use MongoDB;

sub run_test {
    my $test = shift;

    my $valid = $test->{valid};

    my $mc;
    my $err = exception { $mc = MongoDB->connect( $test->{uri} ) };

    if ( !$valid ) {
        isnt( $err, undef, "invalid uri" );
        return;
    }

    is( $err, undef, "valid parse" );

    my $cred = $mc->_credential;
    ok( $cred, "credential created" ) or return;

    if ( !$test->{credential} ) {
        is( $cred->mechanism, "NONE", "credential should not be configured" );
        return;
    }

    my $test_cred = $test->{credential};

    is( $cred->source,    $test_cred->{source},    "source" )
        if exists $test_cred->{source};
    is( uc $cred->mechanism , uc ($test_cred->{mechanism} // "DEFAULT"), "mechanism" )
        if exists $test_cred->{mechanism};
    is( $cred->username,  $test_cred->{username},  "username" )
        if exists $test_cred->{username};
    is( $cred->password,  $test_cred->{password},  "password" )
        if exists $test_cred->{password};
    if ( exists $test_cred->{mechanism_properties} ) {
        my $test_prop = $test_cred->{mechanism_properties};
        my $cred_prop = $cred->mechanism_properties;
        for my $k ( keys %$test_prop ) {
            is( $cred_prop->{$k}, $test_prop->{$k}, "authMechanismProperties: $k" )
        }
    }
}

my $dir      = path("t/data/auth");
my $iterator = $dir->iterator;
my $json     = JSON::MaybeXS->new;
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/;
    my $plan = eval { $json->decode( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    subtest $path => sub {
        for my $test ( @{ $plan->{tests} } ) {
            my $description = $test->{description};
            subtest $description => sub { run_test($test); }
        }
    }
}

done_testing;
