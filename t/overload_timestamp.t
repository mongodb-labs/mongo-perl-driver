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
use Test::More 0.88;

use MongoDB::Timestamp;

my $low = 1;
my $high = 2;

my $ts_low_sec_low_inc   = MongoDB::Timestamp->new( sec => $low, inc => $low );
my $ts_low_sec_low_inc2  = MongoDB::Timestamp->new( sec => $low, inc => $low );
my $ts_high_sec_low_inc  = MongoDB::Timestamp->new( sec => $high, inc => $low );
my $ts_high_sec_high_inc = MongoDB::Timestamp->new( sec => $high, inc => $high );
my $ts_low_sec_high_inc  = MongoDB::Timestamp->new( sec => $low, inc => $high );

ok $ts_low_sec_low_inc    <   $ts_high_sec_low_inc,   '<';
ok $ts_low_sec_low_inc    <=  $ts_low_sec_high_inc,   '<=';
ok $ts_low_sec_low_inc    <=  $ts_low_sec_low_inc2,   '<= identical';
ok $ts_high_sec_low_inc   >   $ts_low_sec_high_inc,   '>';
ok $ts_high_sec_high_inc  >=  $ts_high_sec_low_inc,   '>=';
ok $ts_low_sec_low_inc    >=  $ts_low_sec_low_inc2,   '>= identical';
ok $ts_low_sec_low_inc    ==  $ts_low_sec_low_inc2,   '==';
ok $ts_low_sec_low_inc    !=  $ts_high_sec_low_inc,   '!=';

done_testing;
