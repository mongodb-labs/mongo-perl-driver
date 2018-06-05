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

my $class = "MongoDB::WriteConcern";

require_ok( $class );

is(
    exception { $class->new },
    undef,
    "new without args has default"
);

like(
    exception { $class->new( w => 0, j => 1 ) },
    qr/can't use write concern w=0 with j=1/,
    "j=1 not allowed with w=0",
);

done_testing;

# vim: ts=4 sts=4 sw=4 et:
