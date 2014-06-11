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

plan tests => 11;

my $rs;

SKIP: {
    skip 'requires running replica set', 11 unless exists $ENV{MONGOTEST_PATH};

    $rs = MongoDBTest::ReplicaSet->new(
        mongo_path => $ENV{MONGOTEST_PATH},
        logpath => '/data/db',
        name => 'testset',
        port => 27020,
        set_size => 3,
        priorities => [1, 1, 2]
    );

    # set up replica set tags, and wait for reconfig
    $rs->add_tags({disk => 'ssd', use => 'production', rack => 'f'},
                  {disk => 'ssd', use => 'production', rack => 'k'},
                  {disk => 'spinning', use => 'reporting', mem => '32'});
    sleep 5;

    my $rsconn = $rs->client;
    my $db = $rsconn->get_database('test_database');
    my $collection = $db->get_collection('test_collection');

    $rsconn->read_preference(MongoDB::MongoClient->SECONDARY_PREFERRED, [{rack => 'f'}]);
    my $pinhost = $rsconn->_readpref_pinned->host;
    is($pinhost, 'mongodb://localhost:27020', 'secondary pinned');

    # the host list should have three members
    ok($rsconn->_servers->{'localhost:27020'}, 'localhost:27020 in host list');
    ok($rsconn->_servers->{'localhost:27021'}, 'localhost:27021 in host list');
    ok($rsconn->_servers->{'localhost:27022'}, 'localhost:27022 in host list');
    is(keys %{$rsconn->_servers}, 3, 'no other hosts are in RS config');

    # reconfig so that the replica set only has two members
    my $replcoll = $rsconn->get_database('local')->get_collection('system.replset');
    my $rsconf = $replcoll->find_one();

    ($rsconf->{'version'})++;
    shift @{$rsconf->{'members'}};

    # reconfig will cause connection to be reset,
    # and throw a connection error
    eval {
        $rsconn->get_database('admin')->run_command({'replSetReconfig' => $rsconf});
    };

    # sleep for long enough that rs refresh will take place
    sleep 6;
    lives_ok { $collection->find()->next; } 'repin safely';

    # the host list should now have three members
    ok(!$rsconn->_servers->{'localhost:27020'}, 'localhost:27020 not in host list');
    ok($rsconn->_servers->{'localhost:27021'}, 'localhost:27021 in host list');
    ok($rsconn->_servers->{'localhost:27022'}, 'localhost:27022 in host list');
    is(keys %{$rsconn->_servers}, 2, 'no other hosts are in RS config');

    $pinhost = $rsconn->_readpref_pinned->host;
    is($pinhost, 'mongodb://localhost:27022', 'primary pinned after reconfig');
}

END {
    if ($conn) {
        $conn->get_database('test_database')->drop();
    }
    if ($rs) {
        $rs->shutdown();
    }
}

