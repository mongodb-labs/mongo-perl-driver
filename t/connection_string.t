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

use MongoDB::_URI;

subtest "localhost" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "localhost trailing comma" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://localhost,');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
};

subtest "localhost with username/password" => sub {

    my $uri = MongoDB::_URI->new( uri => 'mongodb://fred:foobar@localhost');
    my @hostpairs = ('localhost:27017');

    is_deeply($uri->hostpairs, \@hostpairs);
    is($uri->username, 'fred');
    is($uri->password, 'foobar');
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

done_testing;
