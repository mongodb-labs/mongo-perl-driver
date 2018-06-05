#  Copyright 2018 - present MongoDB, Inc.
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

# MongoDB documentation examples in Perl.

# NOTE: Developers: Do not change these examples without approval of the
# MongoDB documentation team as they are extracted to populate examples
# on the MongoDB docs website.
#
# Examples use `$db->coll("inventory")` to parallel the shell examples, which
# use `db.inventory`.  Testing commands use a `$coll` variable for more
# idiomatic brevity.

use strict;
use warnings;
use Test::More 0.96;

use MongoDB;
use Tie::IxHash;
use boolean;

use lib "t/lib";
use MongoDBTest qw/
  skip_unless_mongod build_client get_test_db
  server_version server_type
/;

skip_unless_mongod();

my $conn           = build_client();
my $db             = get_test_db($conn);
my $coll           = $db->coll("inventory");
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $cursor;

# We want to show the examples without showing the inserts/updates
# that would make them work in reality and we don't have threads to
# do the work concurrently.  Therefore, we show the example and then
# repeat the work with database operations and tests intermingled.

#<<< No perltidy

subtest "change streams" => sub {
    plan skip_all => '$currentDate operator requires MongoDB 3.6+'
        unless $server_version >= v3.6.0;
    plan skip_all => 'Change Streams require replica set'
        unless $server_type eq 'RSPrimary';

    my $document;
    my $resume_token;
    my @pipeline;

    # Initialize the database
    $db->coll('warmup')->insert_one({});

    # Start Changestream Example 1
    $cursor = $db->coll('inventory')->watch();
    $document = $cursor->next;
    # End Changestream Example 1

    is $document, undef, 'no changes after example 1';
    $coll->insert_one({ username => 'alice' });
    $document = $cursor->next;
    is $document->{fullDocument}{username}, 'alice',
        'found change inserted after example 1';

    # Start Changestream Example 2
    $cursor = $db->coll('inventory')->watch(
        [],
        { fullDocument => 'updateLookup' },
    );
    $document = $cursor->next;
    # End Changestream Example 2

    is $document, undef, 'no changes after example 2';
    $coll->update_one(
        { username => 'alice' },
        { '$set' => { updated => 1 } },
    );
    $document = $cursor->next;
    is $document->{fullDocument}{username}, 'alice',
        'found change made after example 2';

    # Start Changestream Example 3
    $resume_token = $document->{_id};
    $cursor = $db->coll('inventory')->watch(
        [],
        { resumeAfter => $resume_token },
    );
    $document = $cursor->next;
    # End Changestream Example 3

    is $document, undef, 'no changes after example 3';
    $coll->update_one(
        { username => 'alice' },
        { '$set' => { updated => 2 } },
    );
    $document = $cursor->next;
    ok $document, 'found change made after example 3';

    # Start Changestream Example 4
    @pipeline = (
        { '$match' => {
            '$or' => [
                { 'fullDocument.username' => 'alice' },
                { 'operationType' => { '$in' => ['delete'] } },
            ],
        } },
    );
    $cursor = $db->coll('inventory')->watch(\@pipeline);
    $document = $cursor->next;
    # End Changestream Example 4

    is $document, undef, 'no changes after example 4';
    $coll->delete_one({ username => 'alice' });
    $document = $cursor->next;
    ok $document, 'found change made after example 4';

    $coll->drop;
};

#>>> no perltidy

done_testing;
