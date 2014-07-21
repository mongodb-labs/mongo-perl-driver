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

subtest "standalone" => sub {
    plan skip_all => 'needs a standalone server'
        unless $server_type eq 'Standalone';

    ok(!$conn->_readpref_pinned, 'nothing should be pinned yet');
    like(
        exception { $conn->read_preference(MongoDB::MongoClient->PRIMARY); },
        qr/Read preference must be used with a replica set/,
        'read_preference PRIMARY failure on standalone mongod',
    );

    like(
        exception { $conn->read_preference(MongoDB::MongoClient->SECONDARY); },
        qr/Read preference must be used with a replica set/,
        'read_preference SECONDARY failure on standalone mongod',
    );

    ok(!$conn->_readpref_pinned, 'still nothing pinned');

    my $collection = $testdb->get_collection('standalone');
    $collection->drop();
    foreach (1..20) {
        $collection->insert({'a' => $_});
    }

    # make sure we can still query
    is($collection->count(), 20, 'can count the entries');
};


subtest "replica set" => sub {
    plan skip_all => 'needs a replicaset'
        unless $server_type eq 'RSPrimary';

    my $collection = $testdb->get_collection('replicaset');

    {
        $conn->read_preference(MongoDB::MongoClient->PRIMARY);
        is($conn->_master, $conn->_readpref_pinned, 'primary is pinned');
        
        $collection = $conn->get_database('test_database')->get_collection('test_collection');
        my $cursor = $collection->find();
        is($cursor->_client->host, $conn->_master->host, 'cursor connects to primary');
    }

    # check pinning primary with readpref PRIMARY_PREFERRED
    {
        $conn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED);
        is($conn->_master, $conn->_readpref_pinned, 'primary is pinned');
        
        $collection = $conn->get_database('test_database')->get_collection('test_collection');
        my $cursor = $collection->find();
        is($cursor->_client->host, $conn->_master->host, 'cursor connects to primary');
    }

    # check pinning secondary with readpref SECONDARY
    {
        $conn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
        my $pinhost = $conn->_readpref_pinned->host;
        ok($pinhost && $pinhost ne $conn->_master->host, 'secondary is pinned');

        # check pinning secondary with readpref SECONDARY_PREFERRED
        $conn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
        $pinhost = $conn->_readpref_pinned->host;
        ok($pinhost && $pinhost ne $conn->_master->host, 'secondary is pinned');
    }

    # error cases
    {
        like( exception  {
            $conn->read_preference(MongoDB::MongoClient->PRIMARY, [{use => 'production'}]);
        }, qr/PRIMARY cannot be combined with tags/,
        'PRIMARY cannot be combined with tags');

        like( exception  {
            $conn->read_preference()
        }, qr/Missing read preference mode/,
        'Missing read preference mode');

        like( exception  {
            $conn->read_preference(-1)
        }, qr/Unrecognized read preference mode/,
        'bad readpref mode 1');

        like( exception  {
            $conn->read_preference(5);
        }, qr/Unrecognized read preference mode/,
        'bad readpref mode 2');

        like( exception  {
            $conn->read_preference(MongoDB::MongoClient->NEAREST);
        }, qr/NEAREST read preference mode not supported/,
        'NEAREST read preference mode not supported');
    }

    # set read preference on the cursor
    {
        $collection = $conn->get_database('test_database')->get_collection('test_collection');

        my $cursor = $collection->find()->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED);
        is($cursor->_client, $conn->_master, 'call read_preference on cursor');

        $cursor = $collection->find()->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
        isnt($cursor->_client, $conn->_master, 'call read_preference on cursor 2');
    }

    # commands
    {
        $conn->read_preference(MongoDB::MongoClient->SECONDARY);
        isnt($conn->_master, $conn->_readpref_pinned, 'secondary pinned');

        my $admin = $conn->get_database('admin');
        my $cmd = $admin->get_collection('$cmd');

        my $cmd_conn = MongoDB::Collection::_select_cursor_client($conn, 'admin.$cmd',
            Tie::IxHash->new(renameCollection => 'foo.bar', to => 'foo.foofoo'));
        is($cmd_conn, $conn->_master, 'renameCollection runs on primary');

        $cmd_conn = MongoDB::Collection::_select_cursor_client($conn, 'admin.$cmd',
            Tie::IxHash->new(collStats => 'test_database.test_collection', scale => 1024));
        is($cmd_conn, $conn->_readpref_pinned, 'collStats runs on secondary');

        # a command that ignores readpref
        my $cursor = $cmd->find({resetError => 1});
        is($cursor->_master, $conn, 'cursor->_master is ok');
        is($cursor->_client, $conn->_master, 'direct command to _master');
        ok(!$cursor->slave_okay, 'slave_okay false');
        ok(!$cursor->_query->FETCH('$readPreference'), 'no $readPreference field');
        my $cmd_result;
        is( exception { $cmd_result = $admin->run_command({resetError => 1}); }, undef, 'command lives' );
        ok($cmd_result->{'ok'}, 'command ok');

        # a command that obeys read pref
        $cmd = $conn->get_database('test_database')->get_collection('$cmd');
        $cursor = $cmd->find({dbStats => 1, scale => 1024});
        is($cursor->_master, $conn, 'cursor->_master is ok');
        is($cursor->_client, $conn->_readpref_pinned, 'query runs on pinned node');
        ok($cursor->slave_okay, 'slave_okay true');
        ok(!$cursor->_query->FETCH('$readPreference'), 'no $readPreference field');
        is( exception { $cmd_result = $admin->run_command([dbStats => 1, scale => 1024]); }, undef, 'command lives');
        ok($cmd_result->{'ok'}, 'command ok');
    }

};

# connection to mongos
subtest "sharded cluster" => sub {
    plan skip_all => 'requires running sharded environment'
      unless $server_type eq 'Mongos';

    $conn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED, [{foo => 'bar'}]);
    is($conn->_readpref_pinned, $conn, 'mongos pinned');

    # add some data
    my $collection = $testdb->get_collection('shardedcluster');
    foreach (1..300) {
        $collection->insert({a => $_});
    }

    my $cursor = $collection->find();
    is($cursor->_query->FETCH('$readPreference')->{'mode'}, 'primaryPreferred', 'read pref mode added to query');
##    is($cursor->_query->{'$readPreference'}->{'tags'}->[0]->{'foo'},
##       'bar', 'read pref tagsets added to query');

    # make sure we can get the data back
    $conn->read_preference(MongoDB::MongoClient->PRIMARY);
    my $item = $collection->find_one({a => 250});
    is($item->{'a'}, 250, 'querying mongos');
};

done_testing;
