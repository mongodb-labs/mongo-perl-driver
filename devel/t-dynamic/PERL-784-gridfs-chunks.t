#
#  Copyright 2014 MongoDB, Inc.
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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use Try::Tiny;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use if $ENV{MONGOVERBOSE}, qw/Log::Any::Adapter Stderr/;

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs/;

sub _get_orphan_chunks {
    my ($db) = @_;
    
    my @f_ids = $db->get_collection('fs.files')->distinct('_id')->all;
    
    my @orphans = $db->get_collection('fs.chunks')->find({
            files_id => { '$nin' => \@f_ids }
        }, {
            projection => { _id => 1}
        })->all;
    return @orphans;
}

sub _test_orphan_chunks {
    
    local $Test::Builder::Level = $Test::Builder::Level + 1;
    my $conn = build_client( dt_type => undef );
    my $testdb = get_test_db($conn);

    my $coll = $testdb->get_collection('fs.files');
    
    my $file_indexes = $testdb->get_collection('fs.files')->indexes;

    $file_indexes->create_one([tst =>1], {unique => 1});
    
    my $gridfs = $testdb->get_gridfs;
    my $duplicate_meta = {
        tst => 'this_will_break'
    };

    my $test_file = "t/data/gridfs/data.bin";
    open (my $fh, '<:raw', $test_file) or die $!;
    try{
        $gridfs->insert($fh, $duplicate_meta);
        $gridfs->insert($fh, $duplicate_meta);
    } catch {};
    close ($fh);

    my @orphan_chunks = _get_orphan_chunks($testdb);
    
    is(scalar @orphan_chunks, 0, "orphan chunks found");
}

subtest "wire protocol 3" => sub {
    my $orc =
      MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-3.4.yml" );
    diag "starting deployment";
    $orc->start;
    local $ENV{MONGOD} = $orc->as_uri;

    _test_orphan_chunks();
};

clear_testdbs;

done_testing;
