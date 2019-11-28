#  Copyright 2019 - present MongoDB, Inc.
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

{
    package Test::MongoDB::_URI;
    use Moo;
    extends 'MongoDB::_URI';

    has _test_seedlist_args => (
        is => 'rw',
    );

    has _test_seedlist_return => (
        is => 'rw',
        default => sub {[]},
    );

    # Net::DNS is an optional dependency, so the original cannot
    # actually be run in the general test instance.
    # Doesnt stop us from stringing it up for tests though!
    sub _fetch_dns_seedlist {
        my ( $self, @args ) = @_;
        $self->_test_seedlist_args([@args]);
        return @{ $self->_test_seedlist_return };
    }
}

my $class = 'Test::MongoDB::_URI';

subtest "boolean params unchanged" => sub {
    my $uri = new_ok( $class, [
        uri                   => 'mongodb+srv://testmongo.example.com/?ssl=true',
        _test_seedlist_return => [
            [{ target => 'localhost', port => 27017 }],
            {
                retryWrites => 'true',
                retryReads  => 'false',
            },
            0
        ]
    ]);

    is_deeply $uri->_test_seedlist_args, [ 'testmongo.example.com', 'init' ],
        'fetch_dns_seedlist called correctly';

    is_deeply $uri->hostids, [ 'localhost:27017' ],
        "hostids correct";

    is_deeply $uri->options,
        { ssl => 1, retrywrites => 1, retryreads => 0 },
        "options correct";

    subtest "force call srv parsing" => sub {
        $uri->_test_seedlist_return([
            [{ target => 'localhost', port => 27019 }],
            {
                retryWrites => 'false',
                retryReads  => 'true',
            },
            1
        ]);
        my ( $new_uri, $expires ) = $uri->_parse_srv_uri( 'mongodb+srv://testmongo2.example.com/?ssl=true', 'init' );

        is_deeply $uri->_test_seedlist_args, [ 'testmongo2.example.com', 'init' ],
            'fetch_dns_seedlist called correctly';

        like $new_uri, qr!^mongodb://localhost:27019/?!, 'URI Host correct';

        # Cannot use straight comparison as options are hash shuffled
        my ($readsVal) = $new_uri =~ qr/retryReads=(\w*)/;
        is $readsVal, 'true', 'Retry Reads true';
        my ($writesVal) = $new_uri =~ qr/retryWrites=(\w*)/;
        is $writesVal, 'false', 'Retry Writes false';
        my ($sslVal) = $new_uri =~ qr/ssl=(\w*)/;
        is $sslVal, 'true', 'SSL true';

        is $expires, 1, 'expires as expected';
    };
};

subtest "stringification" => sub {
    my $uri = new_ok( $class, [
        uri                   => 'mongodb+srv://testpass@testmongo.example.com',
        _test_seedlist_return => [
            [{ target => 'localhost', port => 27017 }],
            {
                retryWrites => 'true',
                retryReads  => 'false',
            },
            0
        ]
    ]);

    is "$uri", 'mongodb+srv://[**REDACTED**]@testmongo.example.com', 'Stringification for SRV urls correct';
};

done_testing;