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

use utf8;
use Scalar::Util 'reftype';
use boolean;

use MongoDB;

use lib "t/lib";
use MongoDBTest '$conn', '$testdb';

plan tests => 22;



# constructor 
{
    my $bulk = $testdb->get_collection( "test_collection" )->bulk;
    my $coll = $bulk->collection;
    isa_ok $coll, 'MongoDB::Collection';
    is $coll->name, 'test_collection';
}

# find
{
    my $bulk = $testdb->get_collection( "test_collection" )->bulk;
    $bulk->find( { a => 1 } );
    ok ref $bulk->_current_selector;
    my $sel = $bulk->_current_selector;

    is reftype $sel, reftype { };

    is reftype $sel->{query}, reftype { };

    is $sel->{query}{a}, 1;
    is $sel->{upsert}, false;
}

# insert
{ 
    my $bulk = $testdb->get_collection( "test_collection" )->bulk;
    $bulk->insert( { a => 2 } );
    my $ins = $bulk->_inserts;
    ok ref $ins;
    is reftype $ins, reftype [ ];
    is $ins->[0]{a}, 2;
}

# update
{
    my $bulk = $testdb->get_collection( "test_collection" )->bulk;
    $bulk->find( { a => 1 } )->update( { a => 2 } );
    ok ref $bulk->_current_selector;
    my $sel = $bulk->_current_selector;

    is reftype $sel, reftype { };

    is reftype $sel->{query}, reftype { };

    is $sel->{query}{a}, 1;
    is $sel->{upsert}, false;

    my $ups = $bulk->_updates;
    ok ref $ups;
    is reftype $ups, reftype [ ];

    my $doc = $ups->[0];
    is reftype $doc, reftype { };

    is $doc->{q}{a}, 1;
    is $doc->{u}{a}, 2;
    is $doc->{upsert}, false;
    is $doc->{multi}, true;
}
