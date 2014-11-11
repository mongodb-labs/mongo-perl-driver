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
use Test::More 0.96;
use Test::Fatal;
use Test::Warn;

use MongoDB;
use Tie::IxHash;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_type/;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_type = server_type($conn);
my $coll = $testdb->get_collection("test_coll");

# passing in "undef"
my @modes = map { MongoDB::MongoClient->$_ }
  qw( PRIMARY SECONDARY PRIMARY_PREFERRED SECONDARY_PREFERRED NEAREST );

subtest "read preference connection string" => sub {

    my $conn2 = build_client(
        host =>
          "mongodb://localhost/?readPreference=primaryPreferred&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=",
    );
    my $rp = $conn2->read_preference;
    is( $rp->mode, 'primaryPreferred', "mode from" );
    is_deeply(
        $rp->tag_sets,
        [ { dc => 'ny', rack => 1 }, { dc => 'ny'}, {} ],
        "tag set list"
    );

};

subtest "insert and query" => sub {
    for my $m ( @modes ) {
        is(
            exception { $conn->read_preference($m) },
            undef,
            "read_preference '$m' set",
        );

        $coll->drop;
        $coll->insert({'a' => $_}) for 1..20;
        is($coll->count(), 20, "$m: count");
    }
};

subtest "read preference on cursor" => sub {
    for my $m ( @modes ) {
        $coll->drop;
        $coll->insert({'a' => $_}) for 1..20;
        is($coll->find->read_preference($m)->count(), 20, "$m: count");
    }
};

subtest "argument variants" => sub {
    $conn->read_preference( MongoDB::ReadPreference->new( mode => 'secondary_preferred' ) );
    is( $conn->read_preference->mode, 'secondaryPreferred', "read pref from object" );
};


subtest "error cases" => sub {
    like( exception  {
        $conn->read_preference(MongoDB::MongoClient->PRIMARY, [{use => 'production'}]);
    }, qr/A tag set list is not allowed with read preference mode 'primary'/,
    'PRIMARY cannot be combined with a tag set list');

    like( exception  {
        $conn->read_preference(-1)
    }, qr/does not pass the type constraint/,
    'bad readpref mode 1');

    like( exception  {
        $conn->read_preference(5);
    }, qr/does not pass the type constraint/,
    'bad readpref mode 2');
};

subtest 'commands' => sub {
    is(
        exception { $conn->read_preference(MongoDB::MongoClient->SECONDARY) },
        undef,
        "read pref set to secondary without error"
    );

    my $admin = $conn->get_database('admin');
    my $temp_coll = $testdb->get_collection("foo");
    $temp_coll->insert({});
    my $testdb_name = $testdb->name;

    is(
        exception {
            $admin->run_command(
                [ renameCollection => "$testdb_name\.foo", to => "$testdb_name\.foofoo" ] );
        },
        undef,
        "generic helper ran with primary read pref"
    );


    # see if it propagates to the secondary
    my $ok = 0;
    for ( 1 .. 10 ) {
        last if $ok = grep { /foofoo/ } $testdb->collection_names;
        sleep $_;
    }

    ok( $ok, "query also saw changes" );

##    $cmd_conn = MongoDB::Collection::_select_cursor_client($conn, 'admin.$cmd',
##        Tie::IxHash->new(collStats => 'test_database.test_collection', scale => 1024));
##    is($cmd_conn, $conn->_readpref_pinned, 'collStats runs on secondary');
##
##    # a command that ignores readpref
##    my $cursor = $cmd->find({resetError => 1});
##    is($cursor->_master, $conn, 'cursor->_master is ok');
##    is($cursor->_client, $conn->_master, 'direct command to _master');
##    ok(!$cursor->slave_okay, 'slave_okay false');
##    ok(!$cursor->_query->FETCH('$readPreference'), 'no $readPreference field');
##    my $cmd_result;
##    is( exception { $cmd_result = $admin->run_command({resetError => 1}); }, undef, 'command lives' );
##    ok($cmd_result->{'ok'}, 'command ok');
##
##    # a command that obeys read pref
##    $cmd = $conn->get_database('test_database')->get_collection('$cmd');
##    $cursor = $cmd->find({dbStats => 1, scale => 1024});
##    is($cursor->_master, $conn, 'cursor->_master is ok');
##    is($cursor->_client, $conn->_readpref_pinned, 'query runs on pinned node');
##    ok($cursor->slave_okay, 'slave_okay true');
##    ok(!$cursor->_query->FETCH('$readPreference'), 'no $readPreference field');
##    is( exception { $cmd_result = $admin->run_command([dbStats => 1, scale => 1024]); }, undef, 'command lives');
##    ok($cmd_result->{'ok'}, 'command ok');

};

done_testing;
