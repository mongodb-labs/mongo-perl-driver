#
#  Copyright 2009-2013 10gen, Inc.
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



package MongoDBTest;

use strict;
use warnings;

use Exporter 'import';
use MongoDB;
use Test::More;

our @EXPORT_OK = ( '$conn' );
our $conn;

# set up connection if we can
BEGIN { 
    eval { 
        my $host = exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost';
        $conn = MongoDB::MongoClient->new( host => $host, ssl => $ENV{MONGO_SSL} );
    };

    if ( $@ ) { 
        plan skip_all => $@;
        exit 0;
    }
};


# clean up any detritus from failed tests
END { 
    return unless $conn;

    $conn->get_database( 'test_database' )->drop;
};


