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

# Test in t-dynamic as not sure if failover should be tested on install?

use strict;
use warnings;
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;
use boolean;

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    clear_testdbs
    get_unique_collection
    server_version
    server_type
    check_min_server_version
    get_feature_compat_version
/;

my $conn = build_client(
    retry_writes => 1,
    heartbeat_frequency_ms => 60 * 1000,
    # build client modifies this so we set it explicitly to the default
    server_selection_timeout_ms => 30 * 1000,
    server_selection_try_once => 0,
);
my $testdb         = get_test_db($conn);
my $coll = get_unique_collection( $testdb, 'retry_failover' );
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $feat_compat_ver = get_feature_compat_version($conn);

plan skip_all => "standalone servers dont support retryableWrites"
    if $server_type eq 'Standalone';

plan skip_all => "retryableWrites requires featureCompatibilityVersion 3.6 - got $feat_compat_ver"
    if ( $feat_compat_ver < 3.6 );

use Devel::Dwarn;

my $primary = $conn->_topology->current_primary;

my $fail_conn = build_client( host => $primary->address );

my $step_down_conn = build_client();

my $ret = $coll->insert_one( { _id => 1, test => 'value' } );

is $ret->inserted_id, 1, 'write succeeded';

my $result = $coll->find_one( { _id => 1 } );

is $result->{test}, 'value', 'Successful write';

$fail_conn->send_admin_command([
    configureFailPoint => 'onPrimaryTransactionalWrite',
    mode => 'alwaysOn',
]);

# wrapped in eval as this will just drop connection
eval {
    $step_down_conn->send_admin_command([
        replSetStepDown => 60,
        force => true,
    ]);
};
my $err = $@;
isa_ok( $err, 'MongoDB::NetworkError', 'Step down successfully errored' );

# TODO assert that it failed once first
my $post_stepdown_ret = $coll->insert_one( { _id => 2, test => 'again' } );

is $post_stepdown_ret->inserted_id, 2, 'write succeeded';

$fail_conn->send_admin_command([
    configureFailPoint => 'onPrimaryTransactionalWrite',
    mode => 'off',
]);

done_testing;
