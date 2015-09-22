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
use Test::Deep;
use Tie::IxHash;
use boolean;

use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;
use MongoDB::Error;
use MongoDB::WriteConcern;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db server_version server_type/;

skip_unless_mongod();

my $conn = build_client();
my $testdb = get_test_db($conn);
my $db_name = $testdb->name;
my $server_version = server_version($conn);
my $server_type = server_type($conn);;

subtest 'get_database' => sub {
    isa_ok( $conn, 'MongoDB::MongoClient' );

    my $db;
    ok( $db = $conn->get_database($db_name), "get_database(NAME)" );
    isa_ok( $db, 'MongoDB::Database' );

    my $wc = MongoDB::WriteConcern->new( w => 2 );
    ok( $db = $conn->get_database( $db_name, { write_concern => $wc } ),
        "get_database(NAME, OPTIONS)" );
    is( $db->write_concern->w, 2, "DB-level write concern as expected" );

    ok( $db = $conn->get_database( $db_name, { write_concern => { w => 3 } } ),
        "get_database(NAME, OPTIONS)" );
    is( $db->write_concern->w, 3, "DB-level write concern coerces" );

    ok( $db = $conn->get_database( $db_name, { bson_codec => { op_char => '-' } } ),
        "get_database(NAME, OPTIONS)" );
    is( $db->bson_codec->op_char, '-', "DB-level bson_codec coerces" );
};

subtest 'run_command' => sub {

    is( ref $testdb->run_command( [ ismaster => 1 ] ),
        'HASH', "run_command(ARRAYREF) gives HASH" );
    is( ref $testdb->run_command( { ismaster => 1 } ),
        'HASH', "run_command(HASHREF) gives HASH" );
    is( ref $testdb->run_command( Tie::IxHash->new( ismaster => 1 ) ),
        'HASH', "run_command(IxHash) gives HASH" );

    if ( $server_type eq 'RSPrimary' && $conn->_topology->all_servers > 1 ) {
        my $primary = $testdb->run_command( [ ismaster => 1 ] );
        my $secondary = $testdb->run_command( [ ismaster => 1 ], { mode => 'secondary' } );
        isnt( $primary->{me}, $secondary->{me}, "run_command respects explicit read preference" )
            or do { diag explain $primary; diag explain $secondary };
    }

    my $err = exception { $testdb->run_command( { foo => 'bar' } ) };

    if ( $err->code == COMMAND_NOT_FOUND ) {
        pass("error from non-existent command");
    }
    else {
        like(
            $err->message,
            qr/no such cmd|unrecognized command/,
            "error from non-existent command"
        );
    }
};

# collection_names
subtest "collection names" => sub {
    is(scalar $testdb->collection_names, 0, 'no collections');

    my $res = $testdb->list_collections;
    cmp_deeply( [ $res->all ], [], "list_collections has empty cursor" );

    my $coll = $testdb->get_collection('test');

    my $cmd = [ create => "test_capped", capped => 1, size => 10000 ];
    $testdb->run_command($cmd);
    my $cap = $testdb->get_collection("test_capped");

    $coll->indexes->create_one([ name => 1]);
    $cap->indexes->create_one([ name => 1]);

    ok($coll->insert_one({name => 'Alice'}), "create test collection");
    ok($cap->insert_one({name => 'Bob'}), "create capped collection");

    my %names = map {; $_ => 1 } $testdb->collection_names;
    my %got = map { $_->{name} => $_ } $testdb->list_collections( { name => qr/^test/ } )->all;
    for my $k ( qw/test test_capped/ ) {
        ok( exists $names{$k}, "collection_names included $k" );
        ok( exists $got{$k}, "list_collections included $k" );
    }
};

# getlasterror
subtest 'getlasterror' => sub {
    plan skip_all => "MongoDB 1.5+ needed"
        unless $server_version >= v1.5.0;

    $testdb->run_command([ismaster => 1]);
    my $result = $testdb->last_error({fsync => 1});
    is($result->{ok}, 1);
    is($result->{err}, undef);

    $result = $testdb->last_error;
    is($result->{ok}, 1, 'last_error: ok');
    is($result->{err}, undef, 'last_error: err');

    # mongos never returns 'n'
    is($result->{n}, $server_type eq 'Mongos' ? undef : 0, 'last_error: n');
};

# reseterror 
{
    my $result = $testdb->run_command({reseterror => 1});
    is($result->{ok}, 1, 'reset error');
}

# forceerror
{
    my $err = exception{ $testdb->run_command({forceerror => 1}) };

    isa_ok( $err, "MongoDB::DatabaseError" );
}

# XXX eval is deprecated, but we'll leave this test for now
subtest "eval (deprecated)" => sub {
    plan skip_all => "eval not available under auth"
        if $conn->password;
    my $hello = $testdb->eval('function(x) { return "hello, "+x; }', ["world"]);
    is('hello, world', $hello, 'db eval');

    like(
        exception { $testdb->eval('function(x) { xreturn "hello, "+x; }', ["world"]) },
        qr/SyntaxError/,
        'js err'
    );
};

# tie
{
    my $admin = $conn->get_database('admin');
    my %cmd;
    tie( %cmd, 'Tie::IxHash', buildinfo => 1);
    my $result = $admin->run_command(\%cmd);
    is($result->{ok}, 1);
}

done_testing;
