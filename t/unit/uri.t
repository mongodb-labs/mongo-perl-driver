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

use MongoDB::_URI;

subtest "localhost" => sub {
    my @hostpairs = ('localhost:27017');

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost');
    is_deeply($uri->hostpairs, \@hostpairs);

    $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,');
    is_deeply($uri->hostpairs, \@hostpairs, "trailing comma");
};

subtest "db_name" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost/example_db');
    is($uri->db_name, "example_db", "parse db_name");

    $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,/example_db');
    is($uri->db_name, "example_db", "parse db_name with trailing comma on host");

    $uri = MongoDB::_URI->new( uri => 'mongodb://localhost/example_db?');
    is($uri->db_name, "example_db", "parse db_name with trailing ?");

    $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,localhost:27020,localhost:27021/example_db');
    is($uri->db_name, "example_db", "parse db_name, many hosts");

    $uri = MongoDB::_URI->new( uri => 'mongodb://localhost/?');
    is($uri->db_name, "", "no db_name with trailing ?");

};

subtest "localhost with username/password" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://fred:foobar@localhost');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
    is($uri->username, 'fred');
    is($uri->password, 'foobar');
};

# XXX this should really be illegal, I think, but the regex allows it
subtest "localhost with username only" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://fred@localhost');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
    is($uri->username, 'fred');
    is($uri->password, undef);
};

subtest "localhost with username/password and db" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://fred:foobar@localhost/baz');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
    is($uri->username, 'fred');
    is($uri->password, 'foobar');
    is($uri->db_name, 'baz');
};

subtest "localhost with username/password and db (trailing comma)" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://fred:foobar@localhost,/baz');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
    is($uri->username, 'fred');
    is($uri->password, 'foobar');
    is($uri->db_name, 'baz');
};

subtest "localhost with username/password and db (trailing question)" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://fred:foobar@localhost/baz?');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
    is($uri->username, 'fred');
    is($uri->password, 'foobar');
    is($uri->db_name, 'baz');
};

subtest "localhost with empty extras" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://:@localhost/?');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "multiple hostnames" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://example1.com:27017,example2.com:27017');
    my @hostpairs = ('example1.com:27017', 'example2.com:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "multiple hostnames at localhost" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,localhost:27018,localhost:27019');
    my @hostpairs = ('localhost:27017', 'localhost:27018', 'localhost:27019');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "multiple hostnames (localhost/domain)" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,example1.com:27018,localhost:27019');
    my @hostpairs = ('localhost:27017', 'example1.com:27018', 'localhost:27019');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "multiple hostnames (localhost/domain)" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,example1.com:27018,localhost:27019');
    my @hostpairs = ('localhost:27017', 'example1.com:27018', 'localhost:27019');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "percent encoded username and password" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://dog%3Adogston:p%40ssword@localhost');
    my @hostpairs = ('localhost:27017');

    is($uri->username, 'dog:dogston');
    is($uri->password, 'p@ssword');
    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "empty username and password" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://:@localhost');
    is($uri->username, '', "empty username");
    is($uri->password, '', "empty password");
};

subtest "case normalization" => sub {
    my $uri;

    $uri = MongoDB::_URI->new( uri => 'mongodb://eXaMpLe1.cOm:27017,eXAMPLe2.com:27017');
    my @hostpairs = ('example1.com:27017', 'example2.com:27017');
    is_deeply($uri->hostpairs, \@hostpairs, "hostname normalized");

    $uri = MongoDB::_URI->new( uri => 'mongodb://localhost/?ReAdPrEfErEnCe=Primary&wTimeoutMS=1000' );
    is( $uri->options->{readpreference}, 'Primary', "readPreference key normalized" );
    is( $uri->options->{wtimeoutms}, 1000, "wTimeoutMS key normalized" );
};

done_testing;
