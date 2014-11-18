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
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use lib "devel/lib";

use MongoDBTest::Orchestrator;
use MongoDBTest qw/build_client get_test_db clear_testdbs/;

use Log::Any::Adapter qw/Stderr/;

my $orc =
    MongoDBTest::Orchestrator->new( config_file => "devel/config/mongod-2.6.yml" );
diag "starting cluster";
$orc->start;
local $ENV{MONGOD} = $orc->as_uri;

my $conn   = build_client( dt_type => undef );
my $admin  = $conn->get_database("admin");
my $testdb = get_test_db($conn);
my $coll   = $testdb->get_collection("test_collection");

$coll->ensure_index({subject => 'text'});

my $corpus = <<'HERE';
America is a country that doesn't know where it is going but is determined to set a speed record getting there.
The first duty of love is to listen.
Brains, like hearts, go where they are appreciated.
My friends are my estate.
It does not do to dwell on dreams and forget to live.
People are more violently opposed to fur than leather because it's safer to harass rich women than motorcycle gangs.
I don't like composers who think. It gets in the way of their plagiarism.
A man who thinks he has a higher purpose can do terrible things, even to those he professes to love.
Millions long for immortality who don't know what to do with themselves on a rainy Sunday afternoon.
HERE

$coll->insert( { subject => $_ } ) for split /\n/, $corpus;

my $cur = $coll->find({ '$text' => { '$search' => 'love' } });
$cur->fields({score => { '$meta' => 'textScore' }});
$cur->sort({score => { '$meta' => 'textScore' }});

my @docs = $cur->all;

my $max = 99999;
for my $d ( @docs ) {
    my $short = substr($d->{subject},0,20) . "...";
    like( $d->{subject}, qr/love/, "saw love in '$short'" );
    ok( $d->{score} <= $max, "score correctly ordered" );
    $max = $d->{score};
}

clear_testdbs;

done_testing;

