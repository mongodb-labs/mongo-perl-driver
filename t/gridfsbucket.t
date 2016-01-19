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
use File::Compare;
use Encode;

use MongoDB;
use MongoDB::GridFSBucket;
use Path::Tiny;

use lib "t/lib";
use MongoDBTest qw/skip_unless_mongod build_client get_test_db/;

skip_unless_mongod();

my $testdb  = get_test_db( build_client() );
my $txtfile = "t/data/gridfs/input.txt";
my $pngfile = "t/data/gridfs/img.png";
my $bigfile = "t/data/gridfs/big.txt";

my ( $img_id, $img_meta );
my $img_length = -s $pngfile;
my $img_md5    = path($pngfile)->digest("MD5");

my ( $txt_id, $txt_meta );
my $txt_length = -s $txtfile;
my $txt_md5    = path($txtfile)->digest("MD5");

my ( $big_id, $big_meta );
my $big_length = -s $bigfile;
my $big_md5    = path($bigfile)->digest("MD5");

sub setup_gridfs {
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->drop;

    my $img = new IO::File( $pngfile, "r" ) or die $!;
    binmode($img);
    $img_id = $bucket->upload_from_stream( 'img.png', $img );
    $img->close;

    my $txt = new IO::File( $txtfile, "r" ) or die $!;
    binmode($txt);
    $txt_id = $bucket->upload_from_stream( 'input.txt', $txt );
    close $txt;

    my $big = new IO::File( $bigfile, "r" ) or die $!;
    binmode($big);
    $big_id = $bucket->upload_from_stream( 'big.txt', $big );
    close $big;
}

# options
{
    my $bucket = $testdb->get_gridfsbucket;
    cmp_deeply( $bucket->read_preference, $testdb->read_preference, 'read preference' );
    cmp_deeply( $bucket->write_concern,   $testdb->write_concern,   'write concern' );
    is( $bucket->bucket_name,      'fs',       'default bucket name' );
    is( $bucket->chunk_size_bytes, 255 * 1024, 'default chunk size bytes' );
}

# test file upload
{
    my $dumb_str = path($txtfile)->slurp_raw;
    my $bucket   = $testdb->get_gridfsbucket;
    open( my $file, '<', $txtfile ) or die $!;
    ok( my $id = $bucket->upload_from_stream( 'input.txt', $file ),
        'upload small file' );
    my $time = DateTime->now;
    close $file;

    my @chunks = $bucket->_chunks->find( { files_id => $id } )->result->all;
    is( scalar @chunks, 1, 'upload small file has 1 chunk' );
    my $chunk = shift @chunks;
    is( $chunk->{'n'},    0,         'upload small file chunk n' );
    is( $chunk->{'data'}, $dumb_str, 'upload small file data' );

    ok( my $filedoc = $bucket->_files->find_id($id), 'upload small file files document' );
    is( $filedoc->{'md5'},      $txt_md5,    'upload small file md5' );
    is( $filedoc->{'length'},   $txt_length, 'upload small file length' );
    is( $filedoc->{'filename'}, 'input.txt', 'upload small file length' );
    ok( $time->epoch - $filedoc->{'uploadDate'}->epoch < 10,
        'upload small file uploadDate' );

    open( $file, '<', $pngfile ) or die $!;
    binmode($file);
    ok(
        $id = $bucket->upload_from_stream(
            'img.png',
            $file,
            {
                metadata     => { airspeed_velocity => '11m/s' },
                content_type => 'img/png',
                aliases      => ['screenshot.png'],
            }
        ),
        'upload large file'
    );
    $time = DateTime->now;
    seek $file, 0, 0;

    my $chunks =
      $bucket->_chunks->find( { files_id => $id }, { sort => { n => 1 } } )->result;
    my $n = 0;
    subtest 'upload large file' => sub {
        while ( $chunks->has_next ) {
            $chunk = $chunks->next;
            is( $chunk->{'n'}, $n, "upload large file chunk $n n" );
            read $file, $dumb_str, $bucket->chunk_size_bytes;
            is( $chunk->{'data'}, $dumb_str, "upload large file chunk $n data" );
            $n += 1;
        }
    };
    ok( eof $file, 'upload large file whole file' );
    close $file;

    ok( $filedoc = $bucket->_files->find_id($id), 'upload large file files document' );
    is( $filedoc->{'md5'},      $img_md5,    'upload large file md5' );
    is( $filedoc->{'length'},   $img_length, 'upload large file length' );
    is( $filedoc->{'filename'}, 'img.png',   'upload large file filename' );
    ok( $time->epoch - $filedoc->{'uploadDate'}->epoch < 10,
        'upload large file uploadDate' );
    cmp_deeply(
        $filedoc->{metadata},
        { airspeed_velocity => '11m/s' },
        'upload large file metadta'
    );
    is( $filedoc->{'contentType'}, 'img/png', 'upload large file content_type' );
    cmp_deeply( $filedoc->{aliases}, ['screenshot.png'], 'upload large file aliases' );

}

# delete
{
    setup_gridfs;

    my $bucket = $testdb->get_gridfsbucket;
    $bucket->delete($img_id);
    is( $bucket->_files->find_id($img_id),  undef, 'bucket delete files' );
    is( $bucket->_chunks->find_id($img_id), undef, 'bucket delete chunks' );

    # should throw error if file does not exist
    my $error;
    like(
        exception { $bucket->delete('nonsense') },
        qr/FileNotFound: no file found for id .+/,
        'delete nonexistant file',
    );

}

# find
{
    setup_gridfs;

    my $bucket  = $testdb->gfs;
    my $results = $bucket->find( { length => $img_meta->{'length'} } );
    my $file    = $results->next;
    is( $file->{'length'}, $img_meta->{'length'}, "found file length" );
    ok( !$results->has_next, "only one document found" );
}

# drop
{
    setup_gridfs;

    my $bucket = $testdb->get_gridfsbucket;
    $bucket->drop;
    is( $bucket->_files->find_one,  undef, "drop leaves files empty" );
    is( $bucket->_chunks->find_one, undef, "drop leaves chunks empty" );

}

# index creation
{
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->drop;
    $bucket = $testdb->get_gridfsbucket;

    cmp_deeply( [ $bucket->_chunks->indexes->list->all ],
        [], "new bucket doesn't create indexes" );

    my $img = new IO::File( $pngfile, "r" ) or die $!;
    binmode($img);
    $img_id = $bucket->upload_from_stream( 'img.png', $img );
    $img->close;

    my %files_idx  = map { $_->{name} => $_ } $bucket->_files->indexes->list->all;
    my %chunks_idx = map { $_->{name} => $_ } $bucket->_chunks->indexes->list->all;

    my $idx;
    $idx = $files_idx{"filename_1_uploadDate_1"};
    ok(
        $idx && $idx->{unique},
        "unique files index on filename+uploadDate created"
    ) or diag explain $idx;

    $idx = $idx = $chunks_idx{"files_id_1_n_1"};
    ok(
        $idx && $idx->{unique},
        "unique chunks index on files_id+n created"
    ) or diag explain $idx;

    # subsequent writes should not trigger index creation
    no warnings 'redefine';
    local *MongoDB::IndexView::create_one = sub { die "re-indexing shouldn't be called" };

    # next insert should not recreate index
    eval {
        $img = new IO::File( $pngfile, "r" ) or die $!;
        binmode($img);
        $img_id = $bucket->upload_from_stream( 'img2.png', $img );
        $img->close;
    };
    is( $@, "", "upload on same bucket doesn't reindex" );

    # even new gridfs object should not recreate index on first upload
    eval {
        $bucket = $testdb->get_gridfsbucket;
        $img = new IO::File( $pngfile, "r" ) or die $!;
        binmode($img);
        $img_id = $bucket->upload_from_stream( 'img3.png', $img );
        $img->close;
    };
    is( $@, "", "upload on same bucket doesn't reindex" );
}


# download_to_stream
{
    setup_gridfs;

    my $tmp    = Path::Tiny->tempfile;
    my $tmp_fh = $tmp->openw_raw;
    my $bucket = $testdb->get_gridfsbucket;
    $bucket->download_to_stream( $big_id, $tmp_fh );
    isnt( fileno $tmp_fh, undef, 'download_to_stream does not close file handle' );
    close $tmp_fh;
    is( compare( $bigfile, "$tmp" ), 0, 'download_to_stream writes to disk' );
}

# open_download_stream
{
    setup_gridfs;

    my $bucket = $testdb->get_gridfsbucket;
    my $dl_stream;
    my $str;

    $dl_stream = $bucket->open_download_stream($txt_id);
    $dl_stream->read( $str, 4 );
    is( $str, "abc\n", 'simple read' );
    $dl_stream->read( $str, 1, 10 );
    is( $str, "abc\n\0\0\0\0\0\0\n", 'read with null byte padding' );
    $dl_stream->read( $str, 3, -10 );
    is( $str, "azyw", 'read with negative offset' );
    $dl_stream->read( $str, 1, -999 );
    is( $str, "\n", 'read with large negative offset' );

    $dl_stream = $bucket->open_download_stream($txt_id);
    is( $dl_stream->readline, "abc\n", 'readline 1' );
    is( $dl_stream->readline, "\n",    'readline 2' );
    is( $dl_stream->readline, "zyw\n", 'readline 3' );

    $dl_stream = $bucket->open_download_stream($txt_id);
    my @arr = $dl_stream->readline;
    cmp_deeply( \@arr, [ "abc\n", "\n", "zyw\n" ], 'readline in list context' );

    $dl_stream = $bucket->open_download_stream($txt_id);
    {
        local $/ = undef;
        $str = $dl_stream->readline;
        is( $str, "abc\n\nzyw\n", 'readline slurp mode' );
    }

    my $tmp    = Path::Tiny->tempfile;
    my $tmp_fh = $tmp->openw_raw;
    $dl_stream = $bucket->open_download_stream($big_id);

    my $data;
    while ( $dl_stream->read( $data, -s $bigfile ) ) {
        print $tmp_fh $data;
    }
    close $tmp_fh;
    is( compare( $bigfile, $tmp ), 0, 'DownloadStream complex read' );
}

# open_download_stream fh magic
{
    setup_gridfs;

    my $bucket = $testdb->get_gridfsbucket;

    my $dl_stream = $bucket->open_download_stream($txt_id);
    my $fh        = $dl_stream->fh;
    is( fileno($fh), -1, "fileno on fh returns -1" );
    my $result;
    read $fh, $result, 3;
    is( $result, 'abc', 'simple fh read' );

    $dl_stream = $bucket->open_download_stream($img_id);
    $fh        = $dl_stream->fh;
    open( my $png_fh, '<', $pngfile );
    is( compare( $fh, $png_fh ), 0, 'complex fh read' );
    close $png_fh;

    $dl_stream = $bucket->open_download_stream($big_id);
    $fh        = $dl_stream->fh;
    open( my $big_fh, '<', $bigfile );
    my $ok = 1;
    while ( my $line = <$fh> ) {
        if ( $line ne <$big_fh> ) {
            is( $line, <$big_fh>, 'complex fh readline' );
            $ok = 0;
            last;
        }
    }
    ok( $ok, "complex fh readline as expected" );
    do { local $/; <$fh> };
    ok( eof($fh), "EOF" );
    close $fh;
    is( fileno($fh), undef, "fileno on closed fh returns undef" );
    close $big_fh;
}

# DownloadStream close
{
    setup_gridfs;

    no warnings;
    my $bucket = $testdb->get_gridfsbucket;
    my $fh     = $bucket->open_download_stream($big_id)->fh;
    ok( scalar <$fh>, 'fh readline before close' );
    close $fh;
    is( scalar <$fh>, undef, 'fh readline after close' );
}

# Custom chunk sizes
{
    setup_gridfs;

    my $bucket = $testdb->get_gridfsbucket;
    my $uploadstream =
      $bucket->open_upload_stream( 'customChunks.txt', { chunk_size_bytes => 12, }, );

    $uploadstream->print( 'a' x 12 );
    $uploadstream->print( 'b' x 8 );
    my $doc = $uploadstream->close;
    my $id = $uploadstream->id;
    my $doc2 = $bucket->find_id($id);
    $doc2->{uploadDate} = ignore(); # DateTime objects internals can differ :-(
    cmp_deeply( $doc, $doc2, "close returns file document" );
    is( $bucket->_chunks->count( { files_id => $id } ),
        2, 'custom chunk size num chunks' );
    my @results = $bucket->_chunks->find( { files_id => $id } )->all;
    is( $results[0]->{data}, 'a' x 12, 'custom chunk size boundries 1' );
    is( $results[1]->{data}, 'b' x 8,  'custom chunk size boundries 2' );
    my $str;
    is( $bucket->open_download_stream($id)->read( $str, 100 ),
        20, 'custom chunk size read' );
    is( $str, 'a' x 12 . 'b' x 8, 'custom chunk size download' );
}

# Unicode
{
    setup_gridfs;

    my $bucket = $testdb->get_gridfsbucket;
    my $uploadstream =
      $bucket->open_upload_stream( 'unicode.txt', { chunk_size_bytes => 12 }, );
    my $fh = $uploadstream->fh;
    is( fileno($fh), -1, "fileno on open fh returns -1" );
    my $beer    = "\x{1f37a}";
    my $teststr = 'abcdefghijk' . $beer;
    my $testlen = length Encode::encode_utf8($teststr);

    $uploadstream->print($teststr);
    $uploadstream->close;
    is( fileno($fh), undef, "fileno on closed fh returns undef" );
    my $id = $uploadstream->id;
    is( $bucket->_chunks->count( { files_id => $id } ), 2, 'unicode upload' );
    is( $bucket->_files->find_id($id)->{length}, $testlen, 'unicode upload file length' );
    my $str;
    is( $bucket->open_download_stream($id)->read( $str, 100 ),
        $testlen, 'unicode read length' );
    $str = Encode::decode_utf8($str);
    is( $str, $teststr, 'unicode read content' );
}

done_testing;
