#  Copyright 2018 - present MongoDB, Inc.
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
use Test::More 0.96;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use UUID::URandom qw/create_uuid/;

use utf8;
use Tie::IxHash;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;

use MongoDBTest qw/
    build_client
    get_test_db
    server_version
    server_type
    clear_testdbs
    get_unique_collection
    uuid_to_string
/;

my $orc =
MongoDBTest::Orchestrator->new(
  config_file => "devel/config/mongod-3.6-compression-zlib.yml" );
$orc->start;

$ENV{MONGOD} = $orc->as_uri;

print $ENV{MONGOD}, "\n";

my $conn = build_client(
    compressors => ['zlib'],
    zlib_compression_level => 9,
);
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $coll           = $testdb->get_collection('test_collection');

plan skip_all => "Requires MongoDB 3.6"
    if $server_version < v3.6.0;

my $server = $orc->get_server('host1');
my $logfile = $server->logfile;

open my $logfile_fh, '<', $server->logfile
    or die "Unable to read $logfile";

my @init_messages = collect_log_messages();
ok scalar(grep { /zlib is supported/ } @init_messages),
    'zlib is supported';

$coll->insert_one({ value => 23 });
subtest 'compression for insert one' => \&subtest_roundtrip;

$coll->insert_many([{ value => 24 }, { value => 25 }]);
subtest 'compression for insert many' => \&subtest_roundtrip;

$testdb->run_command([getnonce => 1]);
subtest 'no compression on getnonce' => \&subtest_no_compression;

subtest 'connection string' => sub {
    my $client = MongoDB->connect(
        $orc->as_uri.'/?compressors=zlib&zlibCompressionLevel=9',
    );
    is_deeply $client->compressors, ['zlib'], 'compressors';
    is $client->zlib_compression_level, 9, 'zlib compression level';
};

clear_testdbs;

done_testing;

sub subtest_no_compression {
    my @messages = collect_log_messages();
    is scalar(grep { /\bdecompressing message with zlib/i } @messages), 0,
        'no decompressed message';
    is scalar(grep { /\bcompressing message with zlib/i } @messages), 0,
        'no compressed message';
}

sub subtest_roundtrip {
    my @messages = collect_log_messages();
    is scalar(grep { /\bdecompressing message with zlib/i } @messages), 1,
        'decompressed message';
    is scalar(grep { /\bcompressing message with zlib/i } @messages), 1,
        'compressed message';
}

sub collect_log_messages {
    my @messages;
    while (defined(my $line = <$logfile_fh>)) {
        chomp $line;
        push @messages, $line
            if $line =~ m{zlib};
    }
    return @messages;
}
