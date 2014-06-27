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
use Devel::Peek;
use Data::Types qw(:float);
use Tie::IxHash;
use DateTime;

use MongoDB;

my $conn = MongoDB::MongoClient->new(ssl => $ENV{MONGO_SSL});
my $db   = $conn->get_database('test_database');
my $coll = $db->get_collection('test_collection');

sub test_safe_insert {

    mstat;

    for (my $i=0; $i<100000; $i++) {
        $coll->insert({foo => 1, bar => "baz"}, {safe => 1});
        if ($i % 1000 == 0) { 
            print DateTime->now."\n";
            mstat;
        }
    }

    mstat;

}


sub test_insert {

    mstat;

    for (my $i=0; $i<100000; $i++) {
        $coll->insert({foo => 1, bar => "baz"});
        if ($i % 1000 == 0) { 
            print DateTime->now."\n";
            mstat;
        }
    }

    mstat;

}

sub test_id_insert {

    $coll->drop;

    mstat;

    for (my $i=0; $i<100000; $i++) {
        $coll->insert({_id => $i, foo => 1, bar => "baz"});
        if ($i % 1000 == 0) { 
            print DateTime->now."\n";
            mstat;
        }
    }

    mstat;

}
