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
use Config;

BEGIN { plan skip_all => 'requires threads' unless $Config{usethreads} }

use MongoDB;
use Try::Tiny;
use threads;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $conn = build_client();
my $testdb = get_test_db($conn);

my $col = $testdb->get_collection('kooh');
$col->drop;

{

    my $ret = try {
        threads->create(sub {
            $col->insert({ foo => 42 }, { safe => 1 });
        })->join->value;
    }
    catch {
        diag $_;
    };

    ok $ret, 'we survived destruction of a cloned connection';

    my $o = $col->find_one({ foo => 42 });
    is $ret, $o->{_id}, 'we inserted and joined the OID back';
}

{
    my @threads = map {
        threads->create(sub {
            my $col = $conn->get_database($testdb->name)->get_collection('kooh');
            map { $col->insert({ foo => threads->self->tid }, { safe => 1 }) } 0..999;
        })
    } 0 .. 99;

    my @vals = map { ( $_->tid ) x 1000 } @threads;
    my @ids = map { $_->join } @threads;

    is scalar keys %{ { map { ($_ => 1) } @ids } }, scalar @ids,
        'we got 10 unique OIDs';

    is_deeply(
        [map { $col->find_one({ _id => $_ })->{foo} } @ids],
        [@vals],
        'right values inserted from threads',
    );
}

done_testing();
