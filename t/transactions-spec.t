#
#  Copyright 2015 MongoDB, Inc.
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
use Test::More 0.96;

use utf8;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
/;

my @events;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }

my $conn           = build_client();
my $server_version = server_version($conn);
my $server_type    = server_type($conn);

plan skip_all => "Requires MongoDB 4.0"
    if $server_version < v4.0.0;

plan skip_all => "deployment does not support transactions"
    unless $conn->_topology->_supports_transactions;

my $dir      = path("t/data/transactions");
my $iterator = $dir->iterator;
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    my $test_db_name = $plan->{database_name};
    my $test_coll_name = $plan->{collection_name};

    subtest $path => sub {

        for my $test ( @{ $plan->{tests} } ) {
            my $description = $test->{description};
            subtest $description => sub {
                my $client = build_client();

                # Kills its own session as well
                eval { $client->send_admin_command([ killAllSessions => [] ]) };
                my $test_db = $client->get_database( $test_db_name );
                $test_db
                  ->get_collection( $test_coll_name, { write_concern => { w => 'majority' } } )
                  ->drop;

                my $test_coll = $test_db->get_collection( $test_coll_name, { write_concern => { w => 'majority' } } );
use Carp::Always;
                if ( scalar @{ $plan->{data} } > 0 ) {
                    $test_coll->insert_many( $plan->{data} );
                }

                run_test( $test_db_name, $test_coll_name, $test );
            };
        }
    };
}

sub to_snake_case {
  my $t = shift;
  $t =~ s{([A-Z])}{_\L$1}g;
  return $t;
}

sub run_test {
    my ( $test_db_name, $test_coll_name, $test ) = @_;

    my $client_options = $test->{clientOptions} // {};
    # Remap camel case to snake case
    $client_options = {
      map {
        my $k = to_snake_case( $_ );
        $k => $client_options->{ $_ }
      } keys %$client_options
    };

    use Devel::Dwarn; Dwarn $client_options;

    ok 1;
    return;

    my $client = build_client( monitoring_callback => \&event_cb, %$client_options );

    my $session0 = $client->start_session;
    my $lsid0 = $session0->session_id;
    my $session1 = $client->start_session;
    my $lsid1 = $session1->session_id;

    for my $operation ( @{ $test->{operations} } ) {
        eval {
            my $test_db = $client->get_database( $test_db_name );
            my $test_coll = $client->get_collection( $test_coll_name );
            my $cmd = to_snake_case( $operation->{name} );

        }
    }

    $session0->end_session;
    $session1->end_session;

    ok 1;
}

clear_testdbs;

done_testing;
