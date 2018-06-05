#  Copyright 2014 - present MongoDB, Inc.
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

use strict;
use warnings;
use Test::More 0.88;
use Test::Fatal;

use MongoDB::_Server;
use Time::HiRes qw/time/;

my $class = "MongoDB::_Link";

require_ok( $class );

my $obj = new_ok( $class, [ address => 'localhost:27017'] );

my $dummy_server = MongoDB::_Server->new(
    address => 'localhost:27017',
    last_update_time => time,
);

$obj->set_metadata( $dummy_server );

is( $obj->max_bson_object_size, 4*1024*1024, "default max bson object size" );
is( $obj->max_message_size_bytes, 2*4*1024*1024, "default max message size" );

{
    # monkeypatch to let length check fire
    no warnings 'redefine', 'once';

    local *MongoDB::_Link::assert_valid_connection = sub { 1 };
    like(
        exception { $obj->write( "a" x ($obj->max_message_size_bytes + 1) ) },
        qr/Message.*?exceeds maximum/,
        "over long message throws error",
    );
}

done_testing;
# vim: ts=4 sts=4 sw=4 et:
