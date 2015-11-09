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
use MongoDB::GridFSBucket;
use MongoDB::GridFSBucket::DownloadStream;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $testdb = get_test_db(build_client());
my $txtfile = "t/data/gridfs/input.txt";
my $pngfile = "t/data/gridfs/img.png";
my $bigfile = "t/data/gridfs/big.txt";

my $dumb_str;
my $grid;
my ($img_id, $img_meta);
my ($txt_id, $txt_meta);
my ($big_id, $big_meta);

sub setup_gridfs {
    $grid = $testdb->get_gridfs;
    $grid->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    # Windows is dumb
    binmode($img);
    $img_id = $grid->insert($img);
    $img->read($dumb_str, 4000000);
    $img->close;
    $img_meta = $grid->files->find_one({'_id' => $img_id});
    is($img_meta->{'length'}, 1292706);

    my $txt = new IO::File($txtfile, "r") or die $!;
    # Windows is dumb part II
    binmode($txt);
    $txt_id = $grid->insert($txt);
    $txt->read($dumb_str, 100);
    $txt->close;
    $txt_meta = $grid->files->find_one({'_id' => $txt_id});
    is($txt_meta->{'length'}, 9);

    my $big = new IO::File($bigfile, "r") or die $!;
    # Windows is dumb part III
    binmode($big);
    $big_id = $grid->insert($big);
    $big->read($dumb_str, 4000000);
    $big->close;
    $big_meta = $grid->files->find_one({'_id' => $big_id});
    is($big_meta->{'length'}, 2097410);
}

setup_gridfs;

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
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->delete($img_id);
    is($bucket->files->find_id($img_id), undef, 'bucket delete files');
    is($bucket->chunks->find_id($img_id), undef, 'bucket delete chunks');

    # should throw error if file does not exist
    my $error;
    like(
        exception { $bucket->delete('nonsense') },
        qr/found [0-9]+ files instead of 1 for id .+/,
        'delete nonexistant file',
    );

    setup_gridfs;
}

# find
{
    my $bucket = $testdb->get_gridfsbucket;
    my $results = $bucket->find({ length => $img_meta->{'length'} });
    my $file = $results->next;
    is($file->{'length'}, $img_meta->{'length'});
    ok(!$results->has_next);
}

# drop
{
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->drop;
    is($bucket->files->find_one, undef);
    is($bucket->chunks->find_one, undef);

    setup_gridfs;
}

# download_to_stream
{
    my ($tmp_fh, $tmp_filename) = tempfile();
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->download_to_stream($big_id, $tmp_fh);
    isnt(fileno $tmp_fh, undef, 'download_to_stream does not close file handle');
    close $tmp_fh;
    is(compare($bigfile, $tmp_filename), 0, 'download_to_stream writes to disk');
    unlink $tmp_filename;
}

# open_download_stream
{
    my $bucket = $testdb->get_gridfsbucket;
    my $dl_stream;
    my $str;

    $dl_stream = $bucket->open_download_stream($txt_id);
    $dl_stream->read($str, 4);
    is($str, "abc\n", 'simple read');
    $dl_stream->read($str, 1, 10);
    is($str, "abc\n\0\0\0\0\0\0\n", 'read with null byte padding');
    $dl_stream->read($str, 3, -10);
    is($str, "azyw", 'read with negative offset');
    $dl_stream->read($str, 1, -999);
    is($str, "\n", 'read with large negative offset');

    $dl_stream = $bucket->open_download_stream($txt_id);
    is($dl_stream->readline, "abc\n", 'readline 1');
    is($dl_stream->readline, "\n", 'readline 2');
    is($dl_stream->readline, "zyw\n", 'readline 3');

    $dl_stream = $bucket->open_download_stream($txt_id);
    my @arr = $dl_stream->readline;
    cmp_deeply(\@arr, ["abc\n", "\n", "zyw\n"], 'readline in list context');

    $dl_stream = $bucket->open_download_stream($txt_id);
    {
        local $/ = undef;
        $str = $dl_stream->readline;
        is($str, "abc\n\nzyw\n", 'readline slurp mode');
    }

    my ($tmp_fh, $tmp_filename) = tempfile();
    $dl_stream = $bucket->open_download_stream($big_id);

    my $data;
    while ($dl_stream->read($data, 130565)) {
        print $tmp_fh $data;
    }
    close $tmp_fh;
    is(compare($bigfile, $tmp_filename), 0, 'DownloadStream complex read');
    unlink $tmp_filename;
}

# open_download_stream fh magic
{
    my $bucket = $testdb->get_gridfsbucket;

    my $dl_stream = $bucket->open_download_stream($txt_id);
    my $fh = $dl_stream->fh;
    my $result;
    read $fh, $result, 3;
    is($result, 'abc', 'simple fh read');

    $dl_stream = $bucket->open_download_stream($img_id);
    $fh = $dl_stream->fh;
    open(my $png_fh, '<', $pngfile);
    is(compare($fh, $png_fh), 0, 'complex fh read');
    close $png_fh;

    $dl_stream = $bucket->open_download_stream($big_id);
    $fh = $dl_stream->fh;
    open(my $big_fh, '<', $bigfile);
    while (my $line = <$fh>) {
        is($line, <$big_fh>, 'complex fh readline');
    }
    close $big_fh;
}

# close
{
    no warnings;
    my $bucket = $testdb->get_gridfsbucket;
    my $fh = $bucket->open_download_stream($big_id)->fh;
    ok(scalar <$fh>, 'fh readline before close');
    close $fh;
    is(scalar <$fh>, undef, 'fh readline after close');
}

$testdb->drop;
done_testing;
