#  Copyright 2015 - present MongoDB, Inc.
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

package MongoDB::_Constants;

# Common MongoDB driver constants

use version;
our $VERSION = 'v2.2.2';

use Exporter 5.57 qw/import/;
use Config;

my $CONSTANTS;

BEGIN {
    $CONSTANTS = {
        COOLDOWN_SECS                => 5,
        CURSOR_ZERO                  => "\0" x 8,
        EPOCH                        => 0,
        HAS_INT64                    => $Config{use64bitint},
        IDLE_WRITE_PERIOD_SEC        => 10,
        MAX_BSON_OBJECT_SIZE         => 4_194_304,
        MAX_GRIDFS_BATCH_SIZE        => 16_777_216,                 # 16MiB
        MAX_BSON_WIRE_SIZE           => 16_793_600,                 # 16MiB + 16KiB
        MAX_WIRE_VERSION             => 8,
        MAX_WRITE_BATCH_SIZE         => 1000,
        MIN_HEARTBEAT_FREQUENCY_SEC  => .5,
        MIN_HEARTBEAT_FREQUENCY_USEC => 500_000,                    # 500ms, not configurable
        MIN_KEYED_DOC_LENGTH         => 8,
        MIN_SERVER_VERSION           => "2.4.0",
        MIN_WIRE_VERSION             => 0,
        RESCAN_SRV_FREQUENCY_SEC      => $ENV{TEST_MONGO_RESCAN_SRV_FREQUENCY_SEC} || 60,
        NO_JOURNAL_RE                => qr/^journaling not enabled/,
        NO_REPLICATION_RE          => qr/^no replication has been enabled/,
        P_INT32                    => $] lt '5.010' ? 'l' : 'l<',
        SMALLEST_MAX_STALENESS_SEC => 90,
        WITH_ASSERTS               => $ENV{PERL_MONGO_WITH_ASSERTS},
        # Transaction state tracking
        TXN_NONE                    => 'none',
        TXN_STARTING                => 'starting',
        TXN_IN_PROGRESS             => 'in_progress',
        TXN_COMMITTED               => 'committed',
        TXN_ABORTED                 => 'aborted',
        TXN_WTIMEOUT_RETRY_DEFAULT  => 10_000,  # 10 seconds
        TXN_TRANSIENT_ERROR_MSG     => 'TransientTransactionError',
        TXN_UNKNOWN_COMMIT_MSG      => 'UnknownTransactionCommitResult',
        # From the Convenient API for Transactions spec, with_transaction must
        # halt retries after 120 seconds.
        # This limit is non-configurable and was chosen to be twice the 60 second
        # default value of MongoDB's `transactionLifetimeLimitSeconds` parameter.
        WITH_TXN_RETRY_TIME_LIMIT   => 120, # seconds
    };
}

use constant $CONSTANTS;

our @EXPORT = keys %$CONSTANTS;

1;
