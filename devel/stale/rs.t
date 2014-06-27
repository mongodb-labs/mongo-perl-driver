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
use Test::Exception;
use Test::Warn;

use MongoDB;

use lib "t/lib";
use MongoDBTest '$testdb', '$conn', '$server_type';

# this test was extracted from t/rsreadpref.t; it assumes something to spin up
# a replica set, but that code needs to be revised before these tests will run

subtest "three-node replica set" => sub {
    plan skip_all => 'requires running replica set'
      unless exists $ENV{MONGOTEST_PATH};

    $rs = MongoDBTest::ReplicaSet->new(
        mongo_path => $ENV{MONGOTEST_PATH},
        logpath => '/data/db',
        name => 'testset',
        port => 27020,
        set_size => 3,
        priorities => [1, 1, 2]
    );

    my $rsconn = $rs->client;
    
    # set up replica set tags, and wait for reconfig
    $rs->add_tags({disk => 'ssd', use => 'production', rack => 'f'},
                  {disk => 'ssd', use => 'production', rack => 'k'},
                  {disk => 'spinning', use => 'reporting', mem => '32'});
    sleep 5;

    # add a bit of data
    my $database = $rsconn->get_database('test_database');
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

    # make sure Cursor.pm properly rethrows socket exceptions
    # when no read preference has been set yet
    {
        $rs->nodes_down('localhost:27020', 'localhost:27021', 'localhost:27022');
        sleep 2;

        dies_ok {
            $collection->find()->next;
        } 'cursor rethrows socket exception';

        $rs->nodes_up('localhost:27020', 'localhost:27021', 'localhost:27022');
    }

    # wait for election to happen again and reconnect
    sleep 15;
    $rsconn = MongoDB::MongoClient->new(
        host => 'mongodb://localhost:27020',
        find_master => 1
    );


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
    
    # retrieve data from a secondary
    {
        $conn->read_preference(MongoDB::MongoClient->PRIMARY);
        $cursor = $collection->find;
        ok(!$cursor->slave_okay, "don't set slave_okay with readpref PRIMARY");

        $conn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED, [{rack => 'k'}]);
        $pinhost = $conn->_readpref_pinned->host;
        is($pinhost, 'mongodb://localhost:27021', 'secondary pinned');
        
        $cursor = $collection->find;
        ok($cursor->slave_okay, 'cursor should have slave_okay set');

        # kill other nodes to make sure that we really must be communicating
        # with the secondary tagged 'rack => 'k'
        $rs->nodes_down('localhost:27020', 'localhost:27022');
        sleep 2;
        is($collection->find({a => 18})->next()->{'a'}, 18, 'can retrieve data');
        is($collection->find({a => 7})->next()->{'a'}, 7, 'can retrieve data');

        $rs->nodes_up('localhost:27020', 'localhost:27022');
        sleep 10;
    }

};

done_testing;
