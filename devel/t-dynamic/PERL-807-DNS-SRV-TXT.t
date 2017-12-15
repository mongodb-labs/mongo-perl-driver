#
#  Copyright 2009-2013 MongoDB, Inc.
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
#

use strict;
use warnings;
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;

use MongoDBTest qw/build_client get_test_db clear_testdbs server_version
  server_type skip_if_mongod skip_unless_mongod/;

# This test starts servers on localhost ports 27017, 27018 and 27019. We skip if
# these aren't available.
for my $port ( 27017, 27018, 27019 ) {
    local $ENV{MONGOD} = "mongodb://localhost:$port/";
    skip_if_mongod();
}

my $orc =
  MongoDBTest::Orchestrator->new(
    config_file => "devel/config/replicaset-any-27017.yml" );
$orc->start;

sub run_test {
    my $test = shift;

    if ( $test->{error} ) {
        # This test should error the parsing step at some point
        isnt( exception { MongoDB->connect( $test->{uri} ) }, undef,
            "invalid uri" );
        return;
    }

    use Devel::Dwarn;
    Dwarn $test;
    my $mongo = MongoDB->connect( $test->{uri} );
    isa_ok( $mongo, 'MongoDB::MongoClient' );
    my $uri = $mongo->_uri;

    my $lc_opts = { map { lc $_ => $test->{options}->{$_} } keys %{ $test->{options} } };
    # force ssl JSON boolean to perlish
    $lc_opts->{ssl} = $lc_opts->{ssl} ? 1 : 0;
    is_deeply( $uri->options, $lc_opts, "options are correct" );
    is_deeply( [ sort @{ $uri->hostids } ], [ sort @{ $test->{seeds} } ], "seeds are correct" );
    Dwarn $mongo->topology_status( refresh => 1 );
}

my $dir      = path("t/data/initial_dns_seedlist_discovery");
my $iterator = $dir->iterator;
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    subtest $path => sub {
        my $description = $plan->{comment};
        subtest $description => sub {
            run_test( $plan );
        }
    };
    last;
}


clear_testdbs;
done_testing;
