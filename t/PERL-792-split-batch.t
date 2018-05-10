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
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Fatal;

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

my $conn           = build_client(retry_writes => 1);
my $testdb         = get_test_db($conn);
my $coll = get_unique_collection( $testdb, 'retry_split_batch' );
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $feat_compat_ver = get_feature_compat_version($conn);

plan skip_all => "standalone servers dont support retryableWrites"
    if $server_type eq 'Standalone';

plan skip_all => "retryableWrites requires featureCompatibilityVersion 3.6 - got $feat_compat_ver"
    if ( $feat_compat_ver < 3.6 );

subtest "ordered batch split on size" => sub {
    local $TODO = "requires OP_MSG support?";

    enable_failpoint( { mode => { skip => 1 } } );

    # 10MB (ish) doc
    my $big_string = "a" x ( 1024 * 1024 * 10 );
    my $ret = eval {$coll->insert_many( [ map { { _id => $_, a => $big_string } } 0 .. 5 ] ) };
    my $err = $@;

    disable_failpoint();

    use Devel::Dwarn;
    DwarnN $err;
    DwarnN $ret;

    is( $coll->count, 0, "collection count" );
};

sub enable_failpoint {
    my $doc = shift;
    $conn->send_admin_command([
        configureFailPoint => 'onPrimaryTransactionalWrite',
        %$doc,
    ]);
}

sub disable_failpoint {
    my $doc = shift;
    $conn->send_admin_command([
        configureFailPoint => 'onPrimaryTransactionalWrite',
        mode => 'off',
    ]);
}



done_testing;
