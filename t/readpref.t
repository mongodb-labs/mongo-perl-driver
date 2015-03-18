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
use Test::More 0.96;
use Test::Fatal;
use Test::Warn;

use MongoDB;
use Tie::IxHash;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db server_type/;

my $conn = build_client();
my $testdb = get_test_db($conn);
my $server_type = server_type($conn);
my $coll = $testdb->get_collection("test_coll");

# passing in "undef"
my @modes = map { MongoDB::MongoClient->$_ }
  qw( PRIMARY SECONDARY PRIMARY_PREFERRED SECONDARY_PREFERRED NEAREST );

subtest "read preference connection string" => sub {

    my $conn2 = build_client(
        host =>
          "mongodb://localhost/?readPreference=primaryPreferred&readPreferenceTags=dc:ny,rack:1&readPreferenceTags=dc:ny&readPreferenceTags=",
    );
    my $rp = $conn2->read_preference;
    is( $rp->mode, 'primaryPreferred', "mode from" );
    is_deeply(
        $rp->tag_sets,
        [ { dc => 'ny', rack => 1 }, { dc => 'ny'}, {} ],
        "tag set list"
    );

};

subtest "read preference propagation" => sub {
    for my $m (@modes) {
        my $conn2 = build_client( read_preference => { mode => $m } );
        my $db2   = $conn2->get_database( $testdb->name );
        my $coll2 = $db2->get_collection("test_coll");
        my $cur   = $coll2->find( {} );

        for my $thing ( $conn2, $db2, $coll2 ) {
            is( $thing->read_preference->mode, $m, "$m set on " . ref($thing) );
        }
        is( $cur->query->read_preference->mode, $m, "$m set on " . ref($cur) );
    }
};

subtest "read preference on cursor" => sub {
    for my $m ( @modes ) {
        my $cur = $coll->find()->read_preference($m);
        is( $cur->query->read_preference->mode, $m, "$m set on " . ref($cur) );
    }
};

subtest "error cases" => sub {
    like(
        exception { $conn->read_preference( MongoDB::ReadPreference->new ) },
        qr/read-only/,
        "read_preference on client is read-only"
    );

    like(
        exception {
            build_client(
                read_preference => {
                    mode     => 'primary',
                    tag_sets => [ { use => 'production' } ],
                }
              )
        },
        qr/A tag set list is not allowed with read preference mode 'primary'/,
        'primary cannot be combined with a tag set list'
    );
};

subtest 'commands' => sub {

    ok( my $conn2 = build_client( read_preference => 'secondary' ),
        "read pref set to secondary without error" );

    my $admin = $conn2->get_database('admin');

    my $testdb_name = $testdb->name;
    my $db = $conn2->get_database( $testdb_name );

    my $temp_coll = $db->get_collection("foo");
    $temp_coll->insert_one({});

    is(
        exception {
            $admin->run_command(
                [ renameCollection => "$testdb_name\.foo", to => "$testdb_name\.foofoo" ] );
        },
        undef,
        "generic helper ran with primary read pref"
    );

};

done_testing;
