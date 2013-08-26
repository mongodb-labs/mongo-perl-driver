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

use MongoDB;

use lib "t/lib";
use MongoDBTest '$conn';

plan tests => 6;


$conn->w( -1 );
is( $conn->_w_want_safe, 0 );

$conn->w( 0 );
is( $conn->_w_want_safe, 0 );

$conn->w( 1 );
is( $conn->_w_want_safe, 1 );

$conn->w( 'all' );
is( $conn->_w_want_safe, 1 );

$conn->w( 'majority' );
is( $conn->_w_want_safe, 1 );

$conn->w( 'anything' );
is( $conn->_w_want_safe, 1 );
