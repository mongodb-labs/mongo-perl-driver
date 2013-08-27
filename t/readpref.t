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
use Test::More;
use Test::Exception;
use Test::Warn;

use MongoDB;

use lib "t/lib";
use MongoDBTest '$conn';

plan tests => 39;

my $rs;
my $rsconn;
my $sh;

# standalone mongod
{
    ok(!$conn->_readpref_pinned, 'nothing should be pinned yet');
    throws_ok {
        $conn->read_preference(MongoDB::MongoClient->PRIMARY);
    } qr/Read preference must be used with a replica set/,
    'read_preference PRIMARY failure on standalone mongod';

    throws_ok {
        $conn->read_preference(MongoDB::MongoClient->SECONDARY);
    } qr/Read preference must be used with a replica set/,
    'read_preference SECONDARY failure on standalone mongod';

    ok(!$conn->_readpref_pinned, 'still nothing pinned');

    my $database = $conn->get_database('test_database');
    my $collection = $database->get_collection('standalone');
    $collection->drop();
    foreach (1..20) {
        $collection->insert({'a' => $_});
    }

    # make sure we can still query
    is($collection->count(), 20, 'can count the entries');
}

# three-node replica set
SKIP: {
    skip 'requires running replica set', 30 unless exists $ENV{MONGOD_READPREF};

    $rs = MongoDBTest::ReplicaSet->new(mongo_path => $ENV{MONGOD_READPREF},
                                       name => 'testset',
                                       port => 27020,
                                       set_size => 3);

    
    # set up replica set tags, and wait for reconfig
    $rs->add_tags({disk => 'ssd', use => 'production'},
                  {disk => 'ssd', use => 'production', rack => 'k'},
                  {disk => 'spinning', use => 'reporting', mem => '32'});
    sleep 5;

    $rsconn = MongoDB::MongoClient->new(host => 'localhost',
                                        port => 27020,
                                        find_master => 1,
                                        query_timeout => 1000);


    # add a bit of data
    my $database = $conn->get_database('test_database');
    my $collection = $database->get_collection('test_collection');
    $collection->drop();
    foreach (1..20) {
        $collection->insert({'a' => $_});
    }

    my $replcoll = $rsconn->get_database('local')->get_collection('system.replset');
    my $rsconf = $replcoll->find_one();
    is($rsconf->{'members'}->[0]->{'tags'}->{'disk'}, 'ssd', 'check that the config is there');
    is($rsconf->{'members'}->[2]->{'tags'}->{'use'}, 'reporting', 'check config again');

    my $cursor;

    # check pinning primary with readpref PRIMARY
    {
        $rsconn->read_preference(MongoDB::MongoClient->PRIMARY);
        is($rsconn->_master, $rsconn->_readpref_pinned, 'primary is pinned');
        
        $collection = $rsconn->get_database('test_database')->get_collection('test_collection');
        $cursor = $collection->find();
        is($cursor->_client->host, $rsconn->_master->host, 'cursor connects to primary');
    }

    # check pinning primary with readpref PRIMARY_PREFERRED
    {
        $rsconn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED);
        is($rsconn->_master, $rsconn->_readpref_pinned, 'primary is pinned');
        
        $collection = $rsconn->get_database('test_database')->get_collection('test_collection');
        $cursor = $collection->find();
        is($cursor->_client->host, $rsconn->_master->host, 'cursor connects to primary');
    }

    my $pinhost;

    # check pinning secondary with readpref SECONDARY
    {
        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
        $pinhost = $rsconn->_readpref_pinned->host;
        ok($pinhost && $pinhost ne $rsconn->_master->host, 'secondary is pinned');

        # check pinning secondary with readpref SECONDARY_PREFERRED
        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
        $pinhost = $rsconn->_readpref_pinned->host;
        ok($pinhost && $pinhost ne $rsconn->_master->host, 'secondary is pinned');
    }

    # error cases
    {
        throws_ok {
            $rsconn->read_preference(MongoDB::MongoClient->PRIMARY, [{use => 'production'}]);
        } qr/PRIMARY cannot be combined with tags/,
        'PRIMARY cannot be combined with tags';

        throws_ok {
            $rsconn->read_preference();
        } qr/Missing read preference mode/,
        'Missing read preference mode';

        throws_ok {
            $rsconn->read_preference(-1);
        } qr/Unrecognized read preference mode/,
        'bad readpref mode 1';

        throws_ok {
            $rsconn->read_preference(5);
        } qr/Unrecognized read preference mode/,
        'bad readpref mode 2';

        throws_ok {
            $rsconn->read_preference(MongoDB::MongoClient->NEAREST);
        } qr/NEAREST read preference mode not supported/,
        'NEAREST read preference mode not supported';
    }

    # set read preference on the cursor
    {
        $collection = $rsconn->get_database('test_database')->get_collection('test_collection');

        $cursor = $collection->find()->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED);
        is($cursor->_client, $rsconn->_master, 'call read_preference on cursor');

        $cursor = $collection->find()->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED);
        isnt($cursor->_client, $rsconn->_master, 'call read_preference on cursor 2');
    }

    # tagsets
    {
        $rsconn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED, [{foo => 'bar'}]);
        is($rsconn->_readpref_pinned, $rsconn->_master, 'ignore tags if primary is up');

        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY, [{disk => 'ssd', rack => 'k'}]);
        $pinhost = $rsconn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27021', 'tags select mongod on port 27021');

        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY, [{foo => 'bar'}, {rack => 'k'}]);
        $pinhost = $rsconn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27021', 'multiple tagsets');

        throws_ok {
            $rsconn->read_preference(MongoDB::MongoClient->SECONDARY, [{use => 'reporting'}]);
        } qr/No replica set secondary available for query/,
        'tags eliminate all secondaries';

        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED, [{foo => 'bar'}, {a => 'b'}, {c => 'd'}]);
        is($rsconn->_master, $rsconn->_readpref_pinned, 'fallback on primary when no secondaries match');
    }

    # failure tolerance
    {
        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED, [{rack => 'k'}]);
        $pinhost = $rsconn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27021', 'initially secondary is preferred');

        # shutdown the pinned host
        $rs->nodes_down('localhost:27021');
        sleep 2;

        # after hitting a timeout, repin is triggered,
        # without returning an error to the application
        $cursor = $collection->find();
        $cursor->next;
        $pinhost = $rsconn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27022', 'repin successful');

        # if the preferred secondary comes back up,
        # then it should eventually become pinned again
        $rs->nodes_up('localhost:27021');
        sleep 10;
        $cursor = $collection->find();
        $cursor->next;
        $pinhost = $rsconn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27021', 'secondary repinned');

        # if readpref is SECONDARY and both secondaries are down,
        # then return an error to application
        $rsconn->read_preference(MongoDB::MongoClient->SECONDARY);
        isnt($rsconn->_readpref_pinned, $rsconn->_master, 'secondary pinned');

        $rs->nodes_down('localhost:27020', 'localhost:27021');
        sleep 2;
        throws_ok {
            $cursor = $collection->find();
            $cursor->next;
        } qr/No replica set secondary available for query/,
        'secondaries down with readpref SECONDARY';
        $rs->nodes_up('localhost:27020', 'localhost:27021');
        sleep 2;

        $rsconn->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED,
                                 [{disk => 'ssd', use => 'production', rack => 'k'}]);
        $pinhost = $rsconn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27022', 'primary pinned');
        $cursor = $collection->find();
        is($cursor->_client->host, 'mongodb://localhost:27022', 'cursor uses primary');

        $rs->nodes_down('localhost:27022');
        sleep 2;
        $cursor = $collection->find();
        $cursor->next;
        $pinhost = $rsconn->_readpref_pinned->host;
        is($cursor->_client->host, 'mongodb://localhost:27021', 'secondary pinned');

        # bring all nodes down and make sure that
        # repinning raises an error
        $rs->nodes_down('localhost:27020', 'localhost:27021');
        sleep 2;
        throws_ok {
            $cursor = $collection->find();
            $cursor->next;
        } qr/No replica set members available for query/,
        'throw error if no node is available to repin';

        # bring everyone back up, and make sure that the
        # primary ends up pinned
        $rs->nodes_up('localhost:27020', 'localhost:27021', 'localhost:27022');
        sleep 10;
        $cursor = $collection->find();
        $cursor->next;
        is($cursor->_client->_master, $cursor->_client->_readpref_pinned, 'primary repinned');
    }
}

# connection to mongos
SKIP: {
    skip 'requires running sharded environment', 4 unless exists $ENV{MONGOS_READPREF};

    $sh = MongoDBTest::ShardedCluster->new(
        mongo_path => $ENV{MONGOS_READPREF},
        port => 27030,
        shardns => 'testdb.testcoll',
        shardkey => {a => 1}
    );

    $sh->client->read_preference(MongoDB::MongoClient->PRIMARY_PREFERRED, [{foo => 'bar'}]);
    is($sh->client->_readpref_pinned, $sh->client, 'mongos pinned');

    # add some data
    my $collection = $sh->client->get_database('test_database')->get_collection('test_collection');
    foreach (1..300) {
        $collection->insert({a => $_});
    }

    my $cursor = $collection->find();
    is($cursor->_query->{'$readPreference'}->{'mode'}, 'primaryPreferred', 'read pref mode added to query');
    is($cursor->_query->{'$readPreference'}->{'tags'}->[0]->{'foo'},
       'bar', 'read pref tagsets added to query');

    # make sure we can get the data back
    $sh->client->read_preference(MongoDB::MongoClient->PRIMARY);
    my $item = $collection->find_one({a => 250});
    is($item->{'a'}, 250, 'querying mongos');
}

END {
    if ($conn) {
        $conn->get_database('test_database')->drop();
    }
    if ($rsconn) {
        $rsconn->get_database('test_database')->drop();
    }
    if ($rs) {
        $rs->shutdown();
    }
    if ($sh) {
        $sh->shutdown();
    }
}

