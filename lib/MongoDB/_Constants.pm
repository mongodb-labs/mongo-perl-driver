#
#  Copyright 2015 MongoDB, Inc.
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

use 5.008;
use strict;
use warnings;

package MongoDB::_Constants;

# Common MongoDB driver constants

use version;
our $VERSION = 'v0.999.999.5';

use Exporter 5.57 qw/import/;

my $CONSTANTS;

BEGIN {
    $CONSTANTS = {
        COOLDOWN_SECS        => 5,
        MAX_BSON_OBJECT_SIZE => 4_194_304,
        MAX_BSON_WIRE_SIZE   => 16_793_600,                     # 16MiB + 16KiB
        MAX_WRITE_BATCH_SIZE => 1000,
        P_INT32              => $] lt '5.010' ? 'l' : 'l<',
        WITH_ASSERTS         => $ENV{PERL_MONGO_WITH_ASSERTS},
    };
}

use constant $CONSTANTS;

our @EXPORT = keys %$CONSTANTS;

1;
