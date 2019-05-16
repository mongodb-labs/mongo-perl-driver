#  Copyright 2019 - present MongoDB, Inc.
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
use JSON::MaybeXS qw( is_bool decode_json );
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.96;
use Test::Deep;
use Math::BigInt;
use Storable qw( dclone );

use utf8;

use MongoDB;
use MongoDB::_Types qw/
    to_IxHash
/;
use MongoDB::Error;

use lib "t/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
    skip_unless_mongod
    skip_unless_failpoints_available
    skip_unless_transactions
    remap_hashref_to_snake_case
    to_snake_case
    set_failpoint
    clear_failpoint
/;
use MongoDBSpecTest 'foreach_spec_test';

skip_unless_mongod();
skip_unless_transactions();

my $conn = build_client( wtimeout => undef );
my $db = get_test_db($conn);
my $test_coll = get_unique_collection($db, 'txn_convenient_api');

$test_coll->drop;

my $test_session = $conn->start_session({});

subtest 'Custom Error' => sub {
    eval {
        no warnings 'redefine';
        # mock retry timeout
        *MongoDB::ClientSession::_within_time_limit = sub {0};
        $test_session->with_transaction(
            sub {
                MongoDB::Error->throw(
                    message      => 'Custom Error',
                    error_labels => [ 'TransientTransactionError' ],
                )
            },
        );
        1
    } or do {
        my $err = $@;
        like($err, qr/Custom\s+Error/, 'test callback throws custom exception');
    };

};

subtest 'Return Value' => sub {
    # test return value
    my $exp_test_ret_value = 'Foo';
    is(
        $test_session->with_transaction(
            sub {'Foo'},
        ),
        $exp_test_ret_value,
        'test callback returns value'
    );
    is(
        $test_session->with_transaction(
            sub {
                $test_coll->insert_one({});
                return 'Foo'
            },
        ),
        $exp_test_ret_value,
        'test callback returns value'
    );
};

clear_testdbs;

done_testing;