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
use IO::File;
use File::Temp;
use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;
use MongoDB::GridFS;
use MongoDB::GridFS::File;
use DateTime;
use FileHandle;

use lib "t/lib";
use MongoDBTest qw/build_client get_test_db/;

my $testdb = get_test_db(build_client());
my $txtfile = "t/data/gridfs/input.txt";
my $pngfile = "t/data/gridfs/img.png";

plan tests => 62;

my $dumb_str;
my $now;
my $file;
my $save_id;

my $grid = $testdb->get_gridfs;
$grid->drop;

# test ctor prefix
{
    is($testdb->name . '.fs.files', $grid->files->full_name, "no prefix");
    is($testdb->name . '.fs.chunks', $grid->chunks->full_name);

    my $fancy_grid = $testdb->get_gridfs("bar");
    is($testdb->name . '.bar.files', $fancy_grid->files->full_name, "prefix");
    is($testdb->name . '.bar.chunks', $fancy_grid->chunks->full_name);
}

# test text insert
{
    $dumb_str = "abc\n\nzyw\n";
    my $text_doc = new IO::File("$txtfile", "r") or die $!;
    my $ts = DateTime->now;
    my $id = $grid->put($text_doc); # safe mode so we can check MD5
    $text_doc->close;

    my $chunk = $grid->chunks->find_one();
    is(0, $chunk->{'n'});
    is("$id", $chunk->{'files_id'}."", "compare returned id");
    is($dumb_str, $chunk->{'data'}, "compare file content");

    my $md5 = $testdb->run_command(["filemd5" => $chunk->{'files_id'}, "root" => "fs"]);
    $file = $grid->files->find_one();
    ok($file->{'md5'} ne 'd41d8cd98f00b204e9800998ecf8427e', $file->{'md5'});
    is($file->{'md5'}, $md5->{'md5'}, $md5->{'md5'});
    ok($file->{'uploadDate'}->epoch - $ts->epoch < 10);
    is($file->{'chunkSize'}, $MongoDB::GridFS::chunk_size);
    is($file->{'length'}, length $dumb_str, "compare file len");
    is($chunk->{'files_id'}, $file->{'_id'}, "compare ids");
}

# test bin insert
{
    my $img = new IO::File($pngfile, "r") or die $!;
    # Windows is dumb
    binmode($img);
    my $id = $grid->insert($img);
    $save_id = $id;
    $img->read($dumb_str, 4000000);
    $img->close;
    my $meta = $grid->files->find_one({'_id' => $save_id});
    is($meta->{'length'}, 1292706);

    my $chunk = $grid->chunks->find_one({'files_id' => $id});
    is(0, $chunk->{'n'});
    is("$id", $chunk->{'files_id'}."");
    my $len = $MongoDB::GridFS::chunk_size;
    ok(substr($dumb_str, 0, $len) eq substr($chunk->{'data'}, 0, $len), "compare first chunk with file");

    $file = $grid->files->find_one({'_id' => $id});
    is($file->{'length'}, length $dumb_str, "compare file length");
    is($chunk->{'files_id'}, $file->{'_id'}, "compare ids");
}

# test inserting metadata
{
    my $text_doc = new IO::File("$txtfile", "r") or die $!;
    $now = time;
    my $id = $grid->insert($text_doc, {"filename" => "$txtfile", "uploaded" => time, "_id" => 1});
    $text_doc->close;

    is($id, 1);
}

# $grid->files->find_one (NOT $grid->find_one)
{
    $file = $grid->files->find_one({"_id" => 1});
    ok($file, "found file");
    is($file->{"uploaded"}, $now, "compare ts");
    is($file->{"filename"}, "$txtfile", "compare filename");
}

# $grid->find_one
{
    $file = $grid->find_one({"_id" => 1});
    isa_ok($file, 'MongoDB::GridFS::File');
    is($file->info->{"uploaded"}, $now, "compare ts");
    is($file->info->{"filename"}, "$txtfile", "compare filename");
}

#write
{
    my $wfh = IO::File->new("t/output.txt", "+>") or die $!;
    my $written = $file->print($wfh);
    is($written, length "abc\n\nzyw\n");
    $wfh->close();
}

# slurp
{
    is($file->slurp,"abc\n\nzyw\n",'slurp');
}

{
    my $buf;
    my $wfh = IO::File->new("t/output.txt", "<") or die $!;
    $wfh->read($buf, 1000);
    #$wfh->read($buf, length( "abc\n\nzyw\n"));

    is($buf, "abc\n\nzyw\n", "read chars from tmpfile");

    my $wh = IO::File->new("t/outsub.txt", "+>") or die $!;
    my $written = $file->print($wh, 3, 2);
    is($written, 3);
}

# write bindata
{
    $file = $grid->find_one({'_id' => $save_id});
    my $wfh = IO::File->new('t/output.png', '+>') or die $!;
    $wfh->binmode;
    my $written = $file->print($wfh);
    is($written, $file->info->{'length'}, 'bin file length');
}

#all
{
    my @list = $grid->all;
    is(@list, 3, "three files");
    for (my $i=0; $i<3; $i++) {
        isa_ok($list[$i], 'MongoDB::GridFS::File');
    }
    is($list[0]->info->{'length'}, 9, 'checking lens');
    is($list[1]->info->{'length'}, 1292706);
    is($list[2]->info->{'length'}, 9);
}

# remove
{
    is($grid->files->query({"_id" => 1})->has_next, 1, 'pre-remove');
    is($grid->chunks->query({"files_id" => 1})->has_next, 1);
    $file = $grid->remove({"_id" => 1});
    is(int($grid->files->query({"_id" => 1})->has_next), 0, 'post-remove');
    is(int($grid->chunks->query({"files_id" => 1})->has_next), 0);
}

# remove just_one
{
    $grid->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    $grid->insert($img, {"filename" => "garbage.png"});
    $grid->insert($img, {"filename" => "garbage.png"});

    is($grid->files->count, 2);
    $grid->remove({'filename' => 'garbage.png'}, {just_one => 1});
    is($grid->files->count, 1, 'remove just one');

    unlink 't/output.txt', 't/output.png', 't/outsub.txt';
}

# multi-chunk
{
    $grid->drop;

    foreach (1..3) {
        my $txt = "HELLO" x 1_000_000; # 5MB
        
        my $fh = File::Temp->new;
        $fh->printflush( $txt ) or die $!;
        $fh->seek(0, 0);

        $grid->insert( $fh, { filename => $fh->filename } );
        $fh->close() || die $!;
        #file is unlinked by dtor
        
        # now, spot check that we can retrieve the file
        my $gridfile = $grid->find_one( { filename => $fh->filename } );
        my $info = $gridfile->info();
        
        is($info->{length}, 5000000, 'length: '.$info->{'length'});
        is($info->{filename}, $fh->filename, $info->{'filename'});
    }
}

# reading from a big string
{
    $grid->drop;

    my $txt = "HELLO";

    my $basicfh;
    open($basicfh, '<', \$txt);
    
    my $fh = FileHandle->new;
    $fh->fdopen($basicfh, 'r');
    $grid->insert($fh, {filename => 'hello.txt'});

    $file = $grid->find_one;
    is($file->info->{filename}, 'hello.txt');
    is($file->info->{length}, 5);
}

# safe insert
{
    $grid->drop;
    my $img = new IO::File($pngfile, "r") or die $!;
    $img->binmode;
    $grid->insert($img, {filename => 'img.png'}, {safe => boolean::true});

    $file = $grid->find_one;
    is($file->info->{filename}, 'img.png', 'safe insert');
    is($file->info->{length}, 1292706);
    ok($file->info->{md5} ne 'd41d8cd98f00b204e9800998ecf8427e', $file->info->{'md5'});
}

# get, put, delete
{
    $grid->drop;

    my $img = new IO::File($pngfile, "r") or die $!;
    $img->binmode;

    my $id = $grid->put($img, {_id => 'img.png', filename => 'img.png'});
    is($id, 'img.png', "put _id");

    $img->seek(0,0);
    $id = $grid->put($img);
    isa_ok($id, 'MongoDB::OID');

    $img->seek(0,0);
    eval {
        $id = $grid->put($img, {_id => 'img.png', filename => 'img.png'});
    };

    like($@->result->last_errmsg, qr/E11000/, 'duplicate key exception');

    $file = $grid->get('img.png');
    is($file->info->{filename}, 'img.png');
    ok($file->info->{md5} ne 'd41d8cd98f00b204e9800998ecf8427e', $file->info->{'md5'});

    $grid->delete('img.png');

    my $coll = $testdb->get_collection('fs.files');

    $file = $coll->find_one({_id => 1});
    is($file, undef);

    $coll = $testdb->get_collection('fs.chunks');
    $file = $coll->find_one({files_id => 1});
    is($file, undef);
}

