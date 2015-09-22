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
our $VERSION = 'v1.0.2';

use Exporter 5.57 qw/import/;
use Config;

my $CONSTANTS;

BEGIN {
    $CONSTANTS = {
        COOLDOWN_SECS                => 5,
        CURSOR_ZERO                  => "\0" x 8,
        EPOCH                        => 0,
        HAS_INT64                    => $Config{use64bitint},
        MAX_BSON_OBJECT_SIZE         => 4_194_304,
        MAX_BSON_WIRE_SIZE           => 16_793_600,                 # 16MiB + 16KiB
        MAX_WIRE_VERSION             => 3,
        MAX_WRITE_BATCH_SIZE         => 1000,
        MIN_HEARTBEAT_FREQUENCY_SEC  => .5,
        MIN_HEARTBEAT_FREQUENCY_USEC => 500_000,                    # 500ms, not configurable
        MIN_KEYED_DOC_LENGTH         => 8,
        MIN_WIRE_VERSION             => 0,
        NO_JOURNAL_RE                => qr/^journaling not enabled/,
        NO_REPLICATION_RE => qr/^no replication has been enabled/,
        P_INT32           => $] lt '5.010' ? 'l' : 'l<',
        WITH_ASSERTS      => $ENV{PERL_MONGO_WITH_ASSERTS},
    };
}

use constant $CONSTANTS;

our @EXPORT = keys %$CONSTANTS;

1;
