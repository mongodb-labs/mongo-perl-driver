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
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $testdb = get_test_db(build_client());

if ( $^V lt 5.14.0 ) { 
    plan skip_all => 'we need perl 5.14 for regex tests';
}

plan tests => 2;


$testdb->drop;

my $coll = $testdb->get_collection('test_collection');

my $test_regex = eval 'qr/foo/iu';    # eval regex to prevent compile failure on pre-5.14
warning_like { 
    $coll->insert_one( { name => 'foo', test_regex => $test_regex } )
} qr{unsupported regex flag /u}, 'unsupported flag warning';


my ( $doc ) = $coll->find_one( { name => 'foo' } );
is $doc->{test_regex}, qr/foo/i;
