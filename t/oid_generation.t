#
#  Copyright 2016 MongoDB, Inc.
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
use Test::More;
use Test::Fatal;

use IO::File;
use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;
use TestCodecWrapper;

skip_unless_mongod();

my $txtfile = "t/data/gridfs/input.txt";

sub test_with_codec {
    my ( $label, $codec, $oid_class ) = @_;
    local $Test::Builder::Level = $Test::Builder::Level + 1;

    subtest "Given $label" => sub {
        my $conn   = build_client( $codec ? ( bson_codec => $codec ) : () );
        my $testdb = get_test_db($conn);
        my $coll   = $testdb->get_collection('test_collection');

        subtest "When inserting with insert_one" => sub {
            my $result = $coll->insert_one( {} );
            my $id = $result->inserted_id;
            is( ref $id, $oid_class, "inserted_id is $oid_class" );
        };

        subtest "When inserting with insert_many" => sub {
            my $result = $coll->insert_many( [ {}, {} ] );
            my $ids = $result->inserted_ids;
            for my $i ( keys %$ids ) {
                is( ref $ids->{$i}, $oid_class, "inserted_id $i is $oid_class" );
            }
        };

        subtest "When uploading with GridFSBucket" => sub {
            my $bucket = $testdb->get_gridfsbucket;
            my $txt = new IO::File( $txtfile, "r" ) or die $!;
            binmode($txt);
            my $id = $bucket->upload_from_stream( 'input.txt', $txt );
            is( ref $id, $oid_class, "inserted_id is $oid_class" );
        };
    };
}

test_with_codec( "default codec", undef, "BSON::OID" );
test_with_codec(
    "TestCodecWrapper codec",
    TestCodecWrapper->new(),
    "TestCodecWrapper::OID"
);

done_testing;
