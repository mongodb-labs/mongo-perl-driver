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

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/build_client/;

my $conn = build_client();

plan tests => 6;


$conn->w( -1 );
ok( ! $conn->_write_concern->is_acknowledged, "w:-1" );

$conn->w( 0 );
ok( ! $conn->_write_concern->is_acknowledged, "w:0" );

$conn->w( 1 );
ok( $conn->_write_concern->is_acknowledged, "w:1" );

$conn->w( 'all' );
ok( $conn->_write_concern->is_acknowledged, "w:all" );

$conn->w( 'majority' );
ok( $conn->_write_concern->is_acknowledged, "w:majority" );

$conn->w( 'anything' );
ok( $conn->_write_concern->is_acknowledged, "w:anything" );
