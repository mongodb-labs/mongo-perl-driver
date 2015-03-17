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
use Test::More;
use Test::Fatal;
use Test::Warn;

use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;
use DateTime;
use DateTime::Tiny;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client();
my $testdb = get_test_db($conn);

plan tests => 22;


$testdb->drop;

my $now = DateTime->now;

{
    $testdb->get_collection( 'test_collection' )->insert_one( { date => $now } );

    my $date1 = $testdb->get_collection( 'test_collection' )->find_one->{date};
    isa_ok $date1, 'DateTime';
    is $date1->epoch, $now->epoch;
    $testdb->drop;
}

{
    $testdb->get_collection( 'test_collection' )->insert_one( { date => $now } );
    $conn->dt_type( undef );
    my $date3 = $testdb->get_collection( 'test_collection' )->find_one->{date};
    ok( not ref $date3 );
    is $date3, $now->epoch;
    $testdb->drop;
}


{
    $testdb->get_collection( 'test_collection' )->insert_one( { date => $now } );
    $conn->dt_type( 'DateTime::Tiny' );
    my $date2 = $testdb->get_collection( 'test_collection' )->find_one->{date};
    isa_ok( $date2, 'DateTime::Tiny' );
    is $date2->DateTime->epoch, $now->epoch;
    $testdb->drop;
}

{
    $testdb->get_collection( 'test_collection' )->insert_one( { date => $now } );
    $conn->dt_type( 'DateTime::Bad' );
    like( exception { 
            my $date4 = $testdb->get_collection( 'test_collection' )->find_one->{date};
        },
        qr/Invalid dt_type "DateTime::Bad"/i,
        "invalid dt_type throws"
    );
    $testdb->drop;
}

# roundtrips

{
    $conn->dt_type( 'DateTime' );
    my $coll = $testdb->get_collection( 'test_collection' );
    $coll->insert_one( { date => $now } );
    my $doc = $coll->find_one;

    $doc->{date}->add( seconds => 60 );

    $coll->update( { _id => $doc->{_id} }, { date => $doc->{date} } );

    my $doc2 = $coll->find_one;
    is( $doc2->{date}->epoch, ( $now->epoch + 60 ) );
    $testdb->drop;
}


{
    $conn->dt_type( 'DateTime::Tiny' );
    my $dtt_now = DateTime::Tiny->now;
    my $coll = $testdb->get_collection( 'test_collection' );
    $coll->insert_one( { date => $dtt_now } );
    my $doc = $coll->find_one;

    is $doc->{date}->year,   $dtt_now->year;
    is $doc->{date}->month,  $dtt_now->month;
    is $doc->{date}->day,    $dtt_now->day;
    is $doc->{date}->hour,   $dtt_now->hour;
    is $doc->{date}->minute, $dtt_now->minute;
    is $doc->{date}->second, $dtt_now->second;

    $doc->{date} = DateTime::Tiny->from_string( $doc->{date}->DateTime->add( seconds => 30 )->iso8601 );
    $coll->update( { _id => $doc->{_id} }, $doc );

    my $doc2 = $coll->find_one( { _id => $doc->{_id} } );

    is( $doc2->{date}->DateTime->epoch, $dtt_now->DateTime->epoch + 30 );
    $testdb->drop;
}

{
    # test fractional second roundtrip
    $conn->dt_type( 'DateTime' );
    my $coll = $testdb->get_collection( 'test_collection' );
    my $now = DateTime->now;
    $now->add( nanoseconds => 500_000_000 );
    
    $coll->insert_one( { date => $now } );
    my $doc = $coll->find_one;

    is $doc->{date}->year,       $now->year;
    is $doc->{date}->month,      $now->month;
    is $doc->{date}->day,        $now->day;
    is $doc->{date}->hour,       $now->hour;
    is $doc->{date}->minute,     $now->minute;
    is $doc->{date}->second,     $now->second;
    is $doc->{date}->nanosecond, $now->nanosecond;
}
