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

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $testdb = get_test_db(build_client());
my $txtfile = "t/data/gridfs/input.txt";
my $pngfile = "t/data/gridfs/img.png";
my $bigfile = "t/data/gridfs/big.txt";

my ($img_id, $img_meta);
my $img_length = 1292706;
my $img_md5 = 'bc4cd56891f48ddfd214e1348aa9560b';

my ($txt_id, $txt_meta);
my $txt_length = 9;
my $txt_md5 = '0781b93a5faff923c5960c560c44c246';

my ($big_id, $big_meta);
my $big_length = 2097410;
my $big_md5 = '9c2d4555c51dc9ad2ef9fce9167b5f3b';

sub setup_gridfs {
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    # Windows is dumb
    binmode($img);
    $img_id = $bucket->upload_from_stream('img.png', $img);
    $img_meta = $bucket->files->find_one({'_id' => $img_id});
    is($img_meta->{'length'}, $img_length);
    $img->close;

    my $txt = new IO::File($txtfile, "r") or die $!;
    # Windows is dumb part II
    binmode($txt);
    $txt_id = $bucket->upload_from_stream('input.txt', $txt);
    $txt_meta = $bucket->files->find_one({'_id' => $txt_id});
    is($txt_meta->{'length'}, $txt_length);
    close $txt;

    my $big = new IO::File($bigfile, "r") or die $!;
    # Windows is dumb part III
    binmode($big);
    $big_id = $bucket->upload_from_stream('big.txt', $big);
    $big_meta = $bucket->files->find_one({'_id' => $big_id});
    is($big_meta->{'length'}, $big_length);
    close $big;
}

# options
{
    my $bucket = $testdb->get_gridfsbucket;
    cmp_deeply($bucket->read_preference, $testdb->read_preference, 'read preference');
    cmp_deeply($bucket->write_concern, $testdb->write_concern, 'write concern');
    is($bucket->bucket_name, 'fs', 'default bucket name');
    is($bucket->chunk_size_bytes, 255 * 1024, 'default chunk size bytes');
}

# test file upload
{
    my $dumb_str = "abc\n\nzyw\n";
    my $bucket = $testdb->get_gridfsbucket;
    open(my $file, '<', $txtfile) or die $!;
    ok(my $id = $bucket->upload_from_stream('input.txt', $file), 'upload small file');
    my $time = DateTime->now;
    close $file;

    my @chunks = $bucket->chunks->find({ files_id => $id })->result->all;
    is(scalar @chunks, 1, 'upload small file has 1 chunk');
    my $chunk = shift @chunks;
    is($chunk->{'n'}, 0, 'upload small file chunk n');
    is($chunk->{'data'}, $dumb_str, 'upload small file data');

    ok(my $filedoc = $bucket->files->find_id($id), 'upload small file files document');
    is($filedoc->{'md5'}, $txt_md5, 'upload small file md5');
    is($filedoc->{'length'}, $txt_length, 'upload small file length');
    is($filedoc->{'filename'}, 'input.txt', 'upload small file length');
    ok($time->epoch - $filedoc->{'uploadDate'}->epoch < 10, 'upload small file uploadDate');

    open($file, '<', $pngfile) or die $!;
    # Windooooooooooooooowwwwwwwwwwws!
    binmode($file);
    ok($id = $bucket->upload_from_stream('img.png', $file, {
        metadata     => { airspeed_velocity => '11m/s' },
        content_type => 'img/png',
        aliases        => ['screenshot.png'],
    }), 'upload large file');
    $time = DateTime->now;
    seek $file, 0, 0;

    my $chunks = $bucket->chunks->find({ files_id => $id }, { sort => { n => 1 } })->result;
    my $n = 0;
    while ( $chunks->has_next ) {
        $chunk = $chunks->next;
        is($chunk->{'n'}, $n, "upload large file chunk $n n");
        read $file, $dumb_str, $bucket->chunk_size_bytes;
        is($chunk->{'data'}, $dumb_str, "upload large file chunk $n data");
        $n += 1;
    }
    ok(eof $file, 'upload large file whole file');
    close $file;

    ok($filedoc = $bucket->files->find_id($id), 'upload large file files document');
    is($filedoc->{'md5'}, $img_md5, 'upload large file md5');
    is($filedoc->{'length'}, $img_length, 'upload large file length');
    is($filedoc->{'filename'}, 'img.png', 'upload large file filename');
    ok($time->epoch - $filedoc->{'uploadDate'}->epoch < 10, 'upload large file uploadDate');
    cmp_deeply($filedoc->{metadata}, { airspeed_velocity => '11m/s' }, 'upload large file metadta');
    is($filedoc->{'contentType'}, 'img/png', 'upload large file content_type');
    cmp_deeply($filedoc->{aliases}, ['screenshot.png'], 'upload large file aliases');

}

setup_gridfs;

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
