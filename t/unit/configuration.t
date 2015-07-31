#
#  Copyright 2015 MongoDB, Inc.
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

use MongoDB;
use MongoDB::MongoClient;
use MongoDB::BSON;

use constant HAS_DATETIME_TINY => eval { require DateTime::Tiny; 1 };

sub _mc {
    return MongoDB::MongoClient->new(@_);
}

subtest "host and port" => sub {
    my $mc = _mc();
    is( $mc->host,      "mongodb://localhost:27017", "default host is URI" );
    is( $mc->port,      27017,                       "port" );
    is( $mc->_uri->uri, $mc->host,                   "uri matches host" );

    $mc = _mc( host => "example.com" );
    is( $mc->host, "example.com", "host as hostname is preserved" );
    is( $mc->_uri->uri, "mongodb://example.com:27017", "uri gets host" );

    $mc = _mc( host => "example.com", port => 99 );
    is( $mc->host,      "example.com",              "host as hostname is preserved" );
    is( $mc->port,      99,                         "default port changed" );
    is( $mc->_uri->uri, "mongodb://example.com:99", "uri gets both host and port" );

    $mc = _mc( host => "localhost:27018" );
    is( $mc->_uri->uri, "mongodb://localhost:27018", "host as localhost:27018" );

    $mc = _mc( host => "mongodb://example.com", port => 99 );
    is( $mc->host,      "mongodb://example.com", "host as URI is preserved" );
    is( $mc->port,      99,                      "port changed" );
    is( $mc->_uri->uri, $mc->host,               "uri matches host" );
    is_deeply( $mc->_uri->hostpairs, ["example.com:27017"],
        "host pairs ignores changed port" );
};

subtest "auth mechanism and properties" => sub {
    my $mc = _mc();
    is( $mc->auth_mechanism, 'NONE', "default auth_mechanism" );
    is_deeply( $mc->auth_mechanism_properties, {}, "default auth_mechanism_properties" );

    $mc =
      _mc( auth_mechanism => 'MONGODB-CR', auth_mechanism_properties => { foo => 1 } );
    is( $mc->auth_mechanism, 'MONGODB-CR', "custom auth_mechanism" );
    is_deeply(
        $mc->auth_mechanism_properties,
        { foo => 1 },
        "custom auth_mechanism_properties"
    );

    $mc = _mc(
        host => 'mongodb://localhost/?authMechanism=PLAIN&authMechanismProperties=bar:2',
        auth_mechanism            => 'MONGODB-CR',
        auth_mechanism_properties => { foo => 1 },
    );
    is( $mc->auth_mechanism, 'PLAIN', "authMechanism supersedes auth_mechanism" );
    is_deeply(
        $mc->auth_mechanism_properties,
        { bar => 2 },
        "authMechanismProperties supersedes auth_mechanism_properties"
    );

    $mc = _mc(
        sasl           => 1,
        sasl_mechanism => 'PLAIN',
    );
    is( $mc->auth_mechanism, 'PLAIN', "sasl+sasl_mechanism is auth_mechanism default" );

    $mc = _mc(
        auth_mechanism => 'MONGODB-CR',
        sasl           => 1,
        sasl_mechanism => 'PLAIN',
    );
    is( $mc->auth_mechanism, 'MONGODB-CR',
        "auth_mechanism dominates sasl+sasl_mechanism" );
};

subtest bson_codec => sub {
    my $codec = MongoDB::BSON->new( op_char => '-' );

    my $mc = _mc();
    ok( !$mc->bson_codec->prefer_numeric, "default bson_codec object" );

    $mc = _mc( bson_codec => $codec );
    is( $mc->bson_codec->op_char, '-', "bson_codec object" );

    $mc = _mc( bson_codec => { prefer_numeric => 1 } );
    isa_ok( $mc->bson_codec, 'MongoDB::BSON' );
    ok( $mc->bson_codec->prefer_numeric, "bson_codec coerced from hashref" );

    if ( HAS_DATETIME_TINY ) {
        $mc = _mc( dt_type => 'DateTime::Tiny' );
        isa_ok( $mc->bson_codec, 'MongoDB::BSON' );
        ok( $mc->bson_codec->dt_type, "legacy dt_type influences default codec" );
    }
};

subtest connect_timeout_ms => sub {
    my $mc = _mc();
    is( $mc->connect_timeout_ms, 20000, "default connect_timeout_ms" );

    $mc = _mc( timeout => 60000, );
    is( $mc->connect_timeout_ms, 60000, "legacy 'timeout' as fallback" );

    $mc = _mc(
        timeout            => 60000,
        connect_timeout_ms => 30000,
    );
    is( $mc->connect_timeout_ms, 30000, "connect_timeout_ms" );

    $mc = _mc(
        host               => 'mongodb://localhost/?connectTimeoutMS=10000',
        connect_timeout_ms => 30000,
    );
    is( $mc->connect_timeout_ms, 10000, "connectTimeoutMS" );
};

subtest db_name => sub {
    my $mc = _mc();
    is( $mc->db_name, "", "default db_name" );

    $mc = _mc( db_name => "testdb", );
    is( $mc->db_name, "testdb", "db_name" );

    $mc = _mc(
        host    => 'mongodb://localhost/admin',
        db_name => "testdb",
    );
    is( $mc->db_name, "admin", "database in URI" );
};

my %simple_time_options = (
    heartbeat_frequency_ms      => 60000,
    local_threshold_ms          => 15,
    max_time_ms                 => 0,
    server_selection_timeout_ms => 30000,
    socket_check_interval_ms    => 5000,
);

for my $key ( sort keys %simple_time_options ) {
    subtest $key => sub {
        my $mc = _mc();
        is( $mc->$key, $simple_time_options{$key}, "default $key" );

        $mc = _mc( $key => 99999, );
        is( $mc->$key, 99999, "$key" );

        ( my $cs_key = $key ) =~ s/_//g;
        $mc = _mc(
            host => "mongodb://localhost/?$cs_key=88888",
            $key => 99999,
        );
        is( $mc->$key, 88888, "$cs_key" );
    };
}

subtest journal => sub {
    my $mc = _mc();
    ok( !$mc->j, "default j (false)" );

    $mc = _mc( j => 1 );
    ok( $mc->j, "j (true)" );

    $mc = _mc(
        host => 'mongodb://localhost/?journal=false',
        j    => 1,
    );
    ok( !$mc->j, "journal supersedes j" );
};

subtest "read_pref_mode and read_pref_tag_sets" => sub {
    my $mc = _mc();
    is( $mc->read_pref_mode, 'primary', "default read_pref_mode" );
    is_deeply( $mc->read_pref_tag_sets, [ {} ], "default read_pref_tag_sets" );

    my $tag_set_list = [ { dc => 'nyc', rack => 1 }, { dc => 'nyc' } ];
    $mc = _mc(
        read_pref_mode     => 'secondary',
        read_pref_tag_sets => $tag_set_list,
    );
    is( $mc->read_pref_mode, 'secondary', "read_pref_mode" );
    is_deeply( $mc->read_pref_tag_sets, $tag_set_list, "read_pref_tag_sets" );

    $mc = _mc(
        host => 'mongodb://localhost/?readPreference=nearest&readPreferenceTags=dc:sf',
        read_pref_mode     => 'secondary',
        read_pref_tag_sets => $tag_set_list,
    );
    is( $mc->read_pref_mode, 'nearest', "readPreference" );
    is_deeply( $mc->read_pref_tag_sets, [ { dc => 'sf' } ], "readPreferenceTags" );
};

subtest replica_set_name => sub {
    my $mc = _mc();
    is( $mc->replica_set_name, "", "default replica_set_name" );
    is( $mc->_topology->replica_set_name, '', "topology object matches" );

    $mc = _mc( replica_set_name => "repl1" );
    is( $mc->replica_set_name, "repl1", "replica_set_name" );
    is( $mc->_topology->replica_set_name, "repl1", "topology object matches" );

    $mc = _mc(
        host             => 'mongodb://localhost/?replicaSet=repl2',
        replica_set_name => "repl1",
    );
    is( $mc->replica_set_name, "repl2", "replicaSet" );
    is( $mc->_topology->replica_set_name, "repl2", "topology object matches" );
};

subtest socket_timeout_ms => sub {
    my $mc = _mc();
    is( $mc->socket_timeout_ms, 30000, "default socket_timeout_ms" );

    $mc = _mc( query_timeout => 60000, );
    is( $mc->socket_timeout_ms, 60000, "explicit 'query_timeout' as fallback" );

    $mc = _mc(
        query_timeout     => 60000,
        socket_timeout_ms => 40000,
    );
    is( $mc->socket_timeout_ms, 40000, "socket_timeout_ms" );

    $mc = _mc(
        host              => 'mongodb://localhost/?socketTimeoutMS=10000',
        socket_timeout_ms => 40000,
    );
    is( $mc->socket_timeout_ms, 10000, "socketTimeoutMS" );
};

subtest ssl => sub {
    my $mc = _mc();
    ok( !$mc->ssl, "default ssl (false)" );

    $mc = _mc( ssl => 1 );
    ok( $mc->ssl, "ssl (true)" );

    $mc = _mc( ssl => {} );
    ok( $mc->ssl, "ssl (hashref)" );

    $mc = _mc(
        host => 'mongodb://localhost/?ssl=false',
        ssl  => 1,
    );
    ok( !$mc->ssl, "connection string supersedes" );
};

subtest "username and password" => sub {
    my $mc = _mc();
    is( $mc->username, "", "default username" );
    is( $mc->password, "", "default password" );

    $mc = _mc(
        username => "mulder",
        password => "trustno1"
    );
    is( $mc->username, "mulder",   "username" );
    is( $mc->password, "trustno1", "password" );

    $mc = _mc(
        host     => 'mongodb://scully:skeptic@localhost/',
        username => "mulder",
        password => "trustno1"
    );
    is( $mc->username, "scully",  "username from URI" );
    is( $mc->password, "skeptic", "password from URI" );

    $mc = _mc(
        host     => 'mongodb://:@localhost/',
        username => "mulder",
        password => "trustno1"
    );
    is( $mc->username, "",  "username from URI" );
    is( $mc->password, "", "password from URI" );
};

subtest w => sub {
    my $mc = _mc();
    is( $mc->w, 1, "default w" );

    $mc = _mc( w => 2 );
    is( $mc->w, 2, "w:2" );

    $mc = _mc( w => 'majority' );
    is( $mc->w, 'majority', "w:majority" );

    $mc = _mc(
        host => 'mongodb://localhost/?w=0',
        w    => 'majority',
    );
    is( $mc->w, 0, "w from connection string" );

    isnt( exception { _mc( w => {} ) },
        undef, "Setting w to anything but a string or int dies." );
};

subtest wtimeout => sub {
    my $mc = _mc();
    is( $mc->wtimeout, 1000, "default wtimeout" );

    $mc = _mc( wtimeout => 40000, );
    is( $mc->wtimeout, 40000, "wtimeout" );

    $mc = _mc(
        host     => 'mongodb://localhost/?wtimeoutMS=10000',
        wtimeout => 40000,
    );
    is( $mc->wtimeout, 10000, "wtimeoutMS" );
};

subtest "warnings and exceptions" => sub {
    my $warning;
    local $SIG{__WARN__} = sub { $warning = shift };

    my $mc = _mc( host => "mongodb://localhost/?notArealOption=42" );
    like(
        $warning,
        qr/Unsupported option 'notArealOption' in URI/,
        "unknown option warns with original case"
    );

    like(
        exception { _mc( host => "mongodb://localhost/?ssl=" ) },
        qr/expected boolean/,
        'ssl key with invalid value'
    );
};

done_testing;
