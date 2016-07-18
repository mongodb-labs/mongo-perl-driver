#
#  Copyright 2014 MongoDB, Inc.
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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs server_version/;

sub _saw_write_concern {
    my ( $orc, $cmd, $exp ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $seen = $orc->get_server('host1')->grep_log(qr/\Q$cmd\E.*writeConcern/);
    my $verb = $exp ? "saw" : "didn't see";
    ok( not( !!$seen ^ !!$exp ), "$cmd: $verb writeConcern in log" )
      or diag $orc->get_server('host1')->grep_log(qr/\Q$cmd\E/);
    return;
}

sub _test_write_commands {
    my ( $orc, $exp ) = @_;

    local $Test::Builder::Level = $Test::Builder::Level + 1;

    my $conn           = build_client( dt_type => undef, w => 'majority' );
    my $test_db        = $conn->db("test");
    my $coll    = $test_db->coll("test");

    # database drop
    $test_db->drop;
    _saw_write_concern( $orc, "dropDatabase", $exp );

    # insert a doc
    $coll->insert_one( { x => 1 } );

    # drop collection
    $coll->drop;
    _saw_write_concern( $orc, "drop", $exp );

    # create index
    $coll->indexes->create_one( [ x => 1 ] );
    _saw_write_concern( $orc, "createIndexes", $exp );

    # drop index
    $coll->indexes->drop_one( "x_1" );
    _saw_write_concern( $orc, "dropIndexes", $exp );

    # rename collection
    my $coll2 = $coll->rename("test2");
    _saw_write_concern( $orc, "renameCollection", $exp );

    # insert several docs
    $coll->insert_one( { x => $_ } ) for 1 .. 10;

    # aggregate with $out
    my @pipeline = (
        { '$match' => { x => { '$gte' => 5 } } },
        { '$out' => 'testagg' },
    );
    $coll->aggregate( \@pipeline );
    _saw_write_concern( $orc, "aggregate", $exp );

    # catch exceptions
    my $coll3 = $coll->clone( { write_concern => { w => 4, wtimeout => 0 } } );
    my $res = eval { $coll3->drop };
    my $err = $@;
    if ( $exp ) {
        like( $err, qr/WriteConcernError/, "got error with w:4" );
    }
    else {
        is( $err, "", "no error with w:4 (because it wasn't sent)" );
    }
}

subtest "wire protocol 5" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/replicaset-single-any.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    # Check wire protocol 5
    my $conn = build_client( dt_type => undef );
    my $server_version = server_version($conn);
    plan skip_all => "Needs wire protocol 5" unless $server_version ge v3.3.9;

    _test_write_commands( $orc, 1 );

    diag "stopping deployment";
};

subtest "wire protocol 4" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/replicaset-single-3.2.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_write_commands( $orc, 0 );
};

done_testing;
