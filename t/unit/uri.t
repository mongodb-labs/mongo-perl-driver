#  Copyright 2015 - present MongoDB, Inc.
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
use Test::More;
use Test::Fatal;
use Path::Tiny;
use JSON::MaybeXS;

my $class = "MongoDB::_URI";

require_ok($class);

subtest "basic parsing" => sub {
    my $uri =
      new_ok( $class, [ uri => 'mongodb://user:pass@localhost/example_db?w=1' ] );

    is( $uri->username, 'user' );
    is( $uri->password, 'pass' );
    my @hostids = ('localhost:27017');
    is_deeply( $uri->hostids, \@hostids );
    is( $uri->db_name,              'example_db' );
    is( keys( %{ $uri->options } ), 1 );
    is( $uri->options->{w},         1 );

    like( exception { $class->new( uri => 'invalid' ) }, qr/could not be parsed/ );
};

subtest "host list parsing" => sub {
    my ( $uri, @hostids );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost' ] );
    @hostids = ('localhost:27017');
    is_deeply( $uri->hostids, \@hostids, "single hostname" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost,' ] );
    @hostids = ('localhost:27017');
    is_deeply( $uri->hostids, \@hostids, "single hostname with trailing comma" );

    $uri = new_ok( $class, [ uri => 'mongodb://@localhost' ] );
    @hostids = ('localhost:27017');
    is_deeply( $uri->hostids, \@hostids, "hostname with empty auth credentials" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/' ] );
    @hostids = ('localhost:27017');
    is_deeply( $uri->hostids, \@hostids, "hostname with with trailing slash" );

    $uri =
      new_ok( $class, [ uri => 'mongodb://example1.com:27017,example2.com:27017' ] );
    @hostids = ( 'example1.com:27017', 'example2.com:27017' );
    is_deeply( $uri->hostids, \@hostids, "multiple hostnames" );

    $uri =
      new_ok( $class, [ uri => 'mongodb://localhost,localhost:27018,localhost:27019' ] );
    @hostids = ( 'localhost:27017', 'localhost:27018', 'localhost:27019' );
    is_deeply( $uri->hostids, \@hostids, "multiple hostnames at localhost" );

    $uri = new_ok( $class,
        [ uri => 'mongodb://localhost,example1.com:27018,localhost:27019' ] );
    @hostids = ( 'localhost:27017', 'example1.com:27018', 'localhost:27019' );
    is_deeply( $uri->hostids, \@hostids, "multiple hostnames (localhost/domain)" );

    like(
        exception { $class->new( uri => 'mongodb://' ) },
        qr/missing host list/,
        "missing host list"
    );

    like(
        exception { $class->new( uri => 'mongodb:///' ) },
        qr/missing host list/,
        "missing host list, with trailing slash"
    );

    like(
        exception { $class->new( uri => 'mongodb:///?' ) },
        qr/missing host list/,
        "missing host list, with trailing slash and question mark"
    );

    like(
        exception { $class->new( uri => 'mongodb://local?host' ) },
        qr/could not be parsed/,
        "host list contains unescaped question mark"
    );
};

subtest "hostname normalization and validation" => sub {
    my ( $uri, @hostids );

    $uri =
      new_ok( $class, [ uri => 'mongodb://eXaMpLe1.cOm:27017,eXAMPLe2.com:27017' ] );
    @hostids = ( 'example1.com:27017', 'example2.com:27017' );
    is_deeply( $uri->hostids, \@hostids, "hostname normalized for case" );

    $uri = new_ok( $class, [ uri => 'mongodb://local%68ost' ] );
    @hostids = ('localhost:27017');
    is_deeply( $uri->hostids, \@hostids, "hostname url decoded" );

    $uri = new_ok( $class, [ uri => 'mongodb://hostwithembeddedc%6fmma' ] );
    @hostids = ("hostwithembeddedcomma:27017");
    is_deeply( $uri->hostids, \@hostids, "hostname can contain embedded comma" );

    like(
        exception { $class->new( uri => 'mongodb://:27017' ) },
        qr/contains empty host/,
        "empty hostname"
    );

    like(
        exception { $class->new( uri => 'mongodb://@:27017' ) },
        qr/contains empty host/,
        "empty hostname, with leading at sign"
    );

    like(
        exception { $class->new( uri => 'mongodb://%2Fa.sock' ) },
        qr/Unix domain sockets are not supported/,
        "unix domain socket (unsupported) with absolute path"
    );

    like(
        exception { $class->new( uri => 'mongodb://a%2Fb.sock' ) },
        qr/Unix domain sockets are not supported/,
        "unix domain socket (unsupported) with relative path"
    );

    like(
        exception { $class->new( uri => 'mongodb://a%2fb.sock' ) },
        qr/Unix domain sockets are not supported/,
        "unix domain socket (unsupported) with alternate URL encoding"
    );

    like(
        exception { $class->new( uri => 'mongodb://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]' ) },
        qr/IP literals are not supported/,
        "ip literal (unsupported)"
    );

    like(
        exception { $class->new( uri => 'mongodb://[::1]' ) },
        qr/IP literals are not supported/,
        "ip literal (unsupported), short localhost"
    );

    like(
        exception { $class->new( uri => 'mongodb://[::1]:27017' ) },
        qr/IP literals are not supported/,
        "ip literal (unsupported) with port"
    );

    like(
        exception { $class->new( uri => 'mongodb://[::1]:' ) },
        qr/IP literals are not supported/,
        "ip literal (unsupported) with empty port"
    );

    like(
        exception { $class->new( uri => 'mongodb://localhost:/' ) },
        qr/invalid port/,
        "hostname with empty port"
    );

    like(
        exception { $class->new( uri => 'mongodb://example.com:http' ) },
        qr/invalid port/,
        "non-numeric port"
    );

    like(
        exception { $class->new( uri => 'mongodb://example.com:-1' ) },
        qr/invalid port/,
        "negative port"
    );

    like(
        exception { $class->new( uri => 'mongodb://example.com:0' ) },
        qr/invalid port.*must be in range/,
        "port of 0"
    );

    like(
        exception { $class->new( uri => 'mongodb://example.com:65536' ) },
        qr/invalid port.*must be in range/,
        "port of 65536"
    );
};

subtest "db_name" => sub {
    my $uri = new_ok( $class, [ uri => 'mongodb://localhost/example_db' ] );
    is( $uri->db_name, 'example_db', "parse db_name" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost,/example_db' ] );
    is( $uri->db_name, 'example_db', "parse db_name with trailing comma on host" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/example_db?' ] );
    is( $uri->db_name, 'example_db', "parse db_name with trailing question mark" );

    $uri = new_ok( $class,
        [ uri => 'mongodb://localhost,localhost:27020,localhost:27021/example_db' ] );
    is( $uri->db_name, 'example_db', "parse db_name, many hosts" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost' ] );
    is( $uri->db_name, '', "no db_name with trailing ?" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/' ] );
    is( $uri->db_name, '', "no db_name with trailing /" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/?' ] );
    is( $uri->db_name, '', "no db_name with trailing /?" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/a%62c' ] );
    is( $uri->db_name, 'abc', "db_name properly unescaped" );

    like(
        exception { $class->new( uri => 'mongodb://localhost//' ) },
        qr/database name must be URL encoded, found unescaped '\/'/,
        "database with unescaped slash"
    );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/?authSource=foo' ] );
    is( $uri->db_name, 'foo', "parse db_name from authSource option" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost/example_db?authSource=foo' ] );
    is( $uri->db_name, 'foo', "parse db_name authSource override URI db_name" );
};

subtest "auth credentials" => sub {
    my $uri;

    $uri = new_ok( $class, [ uri => 'mongodb://fred:foobar@localhost' ] );
    is( $uri->username, 'fred',   "basic username parsing" );
    is( $uri->password, 'foobar', "basic password parsing" );

    $uri = new_ok( $class, [ uri => 'mongodb://fred@localhost' ] );
    is( $uri->username, 'fred', "username when no password present" );
    is( $uri->password, undef,  "undefined password, when password not given" );

    $uri = new_ok( $class, [ uri => 'mongodb://:@localhost' ] );
    is( $uri->username, '', "empty username" );
    is( $uri->password, '', "empty password" );

    $uri = new_ok( $class, [ uri => 'mongodb://@localhost' ] );
    is( $uri->username, '', "empty username, when password not given" );
    is( $uri->password, undef,
        "undefined password, when username empty and password not given" );

    $uri = new_ok( $class, [ uri => 'mongodb://localhost' ] );
    is( $uri->username, undef, "undefined username, when no credentials given" );
    is( $uri->password, undef, "undefined password, when no credentials given" );

    $uri = new_ok( $class, [ uri => 'mongodb://dog%3Adogston:p%40ssword@localhost' ] );
    is( $uri->username, 'dog:dogston', "percent encoded username" );
    is( $uri->password, 'p@ssword',    "percent encoded password" );

    like(
        exception { $class->new( uri => 'mongodb://user@name:password@localhost' ) },
        qr/username must be URL encoded/,
        'username with unescaped at sign'
    );

    like(
        exception { $class->new( uri => 'mongodb://username:pass:word@localhost' ) },
        qr/password must be URL encoded/,
        "password with unescaped colon"
    );
};

subtest "options" => sub {
    my ( $uri, @warnings );
    my $dir = path('t/data/uri/');
    my $iterator = $dir->iterator( { recurse => 1 } );
    while ( my $path = $iterator->() ) {
      next unless -f $path && $path =~ /\.json$/;
        my $plan = decode_json( $path->slurp_utf8 );
        foreach my $test ( @{ $plan->{'tests'} } ) {
            ok($test->{'uri'}, $test->{'description'});
            {
                @warnings = ();
                local $SIG{__WARN__} = sub {  push @warnings, $_[0] };
                if ($test->{'valid'}) {
                  $uri = new_ok(
                      $class, [ uri => $test->{'uri'} ], $test->{'description'}
                  );
                  if ( $uri ) {
                      is_deeply( $uri->options, $test->{'options'},
                                 $test->{'description'} );
                  }
                  if ($test->{'warning'}) {
                      ok(scalar @warnings, 'URI has warnings');
                  }
                  else {
                      ok(scalar @warnings == 0, 'URI has no warnings');
                  }
                }
                else {
                    ok( exception { $class->new( uri => $test->{'uri'} ) },
                        $test->{'description'} );
                }
            }
        }
    }
};

done_testing;
