#
#  Copyright 2009-2015 MongoDB, Inc.
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
use IO::File;
use File::Temp qw(tempfile);
use File::Compare;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $testdb = get_test_db(build_client());
my $txtfile = "t/data/gridfs/input.txt";
my $pngfile = "t/data/gridfs/img.png";

my $dumb_str;

# options
{
    my $bucket = $testdb->get_gridfsbucket;
    cmp_deeply($bucket->read_preference, $testdb->read_preference, 'read preference');
    cmp_deeply($bucket->write_concern, $testdb->write_concern, 'write concern');
    is($bucket->bucket_name, 'fs', 'default bucket name');
    is($bucket->chunk_size_bytes, 255 * 1024, 'default chunk size bytes');
}

# delete
{
    my $grid = $testdb->get_gridfs;
    $grid->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    # Windows is dumb
    binmode($img);
    my $id = $grid->insert($img);
    my $save_id = $id;
    $img->read($dumb_str, 4000000);
    $img->close;
    my $meta = $grid->files->find_one({'_id' => $save_id});
    is($meta->{'length'}, 1292706);

    my $bucket = $testdb->get_gridfsbucket;
    $bucket->delete($save_id);
    is($grid->get($save_id), undef, 'bucket delete files');
    is($bucket->chunks->find_one, undef, 'bucket delete chunks');

    # should throw error if file does not exist
    my $error;
    like(
        exception { $bucket->delete('nonsense') },
        qr/found [0-9]+ files instead of 1 for id .+/,
        'delete nonexistant file',
    );
}

# find
{
    my $grid = $testdb->get_gridfs;
    $grid->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    # Windows is dumb
    binmode($img);
    my $id = $grid->insert($img);
    my $save_id = $id;
    $img->read($dumb_str, 4000000);
    $img->close;
    my $meta = $grid->files->find_one({'_id' => $save_id});
    is($meta->{'length'}, 1292706);

    my $bucket = $testdb->get_gridfsbucket;
    my $results = $bucket->find({ length => $meta->{'length'} });
    my $file = $results->next;
    is($file->{'length'}, $meta->{'length'});
    ok(!$results->has_next);
}

# drop
{
    my $grid = $testdb->get_gridfs;
    $grid->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    # Windows is dumb
    binmode($img);
    my $id = $grid->insert($img);
    my $save_id = $id;
    $img->read($dumb_str, 4000000);
    $img->close;
    my $meta = $grid->files->find_one({'_id' => $save_id});
    is($meta->{'length'}, 1292706);

    my $bucket = $testdb->get_gridfsbucket;
    $bucket->drop;
    is($bucket->files->find_one, undef);
    is($bucket->chunks->find_one, undef);
}

$testdb->drop;
done_testing;
