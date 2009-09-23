use strict;
use warnings;
use Test::More tests => 37;
use Test::Exception;
use IO::File;

use MongoDB;
use MongoDB::GridFS;
use MongoDB::GridFS::File;

my $m = MongoDB::Connection->new;
my $db = $m->get_database('foo');
my $grid = $db->get_gridfs;
$grid->drop;

# test ctor prefix
is('foo.fs.files', $grid->files->full_name, "no prefix");
is('foo.fs.chunks', $grid->chunks->full_name);

my $fancy_grid = $db->get_gridfs("bar");
is('foo.bar.files', $fancy_grid->files->full_name, "prefix");
is('foo.bar.chunks', $fancy_grid->chunks->full_name);

# test text insert
my $dumb_str = "abc\n\nzyw\n";
my $text_doc = new IO::File("t/input.txt", "r") or die $!;
my $id = $grid->insert($text_doc);
$text_doc->close;

my $chunk = $grid->chunks->find_one();
is(0, $chunk->{'n'});
is("$id", $chunk->{'files_id'}."", "compare returned id");
is($dumb_str, $chunk->{'data'}, "compare file content");

my $md5 = $db->run_command({"filemd5" => $chunk->{'files_id'}, "root" => "foo.fs.files"});
my $file = $grid->files->find_one();
is($file->{'md5'}, $md5->{'md5'});
is($file->{'length'}, length $dumb_str, "compare file len");
is($chunk->{'files_id'}, $file->{'_id'}, "compare ids");

# test bin insert
my $img = new IO::File("t/img.png", "r") or die $!;
$id = $grid->insert($img);
my $save_id = $id;
$img->read($dumb_str, 4000000);
$img->close;

$chunk = $grid->chunks->find_one({'files_id' => $id});
is(0, $chunk->{'n'});
is("$id", $chunk->{'files_id'}."");
my $len = 1048576;
is(substr($dumb_str, 0, $len), substr($chunk->{'data'}, 0, $len), "compare first chunk with file");

$file = $grid->files->find_one({'_id' => $id});
is($file->{'length'}, length $dumb_str, "compare file length");
is($chunk->{'files_id'}, $file->{'_id'}, "compare ids");

# test inserting metadata
$text_doc = new IO::File("t/input.txt", "r") or die $!;
my $now = time;
$id = $grid->insert($text_doc, {"filename" => "t/input.txt", "uploaded" => time, "_id" => 1});
$text_doc->close;

is($id, 1);
# NOT $grid->find_one
$file = $grid->files->find_one({"_id" => 1});
ok($file, "found file");
is($file->{"uploaded"}, $now, "compare ts");
is($file->{"filename"}, "t/input.txt", "compare filename");

# find_one
$file = $grid->find_one({"_id" => 1});
isa_ok($file, 'MongoDB::GridFS::File');
is($file->info->{"uploaded"}, $now, "compare ts");
is($file->info->{"filename"}, "t/input.txt", "compare filename");

#write
my $wfh = IO::File->new("t/output.txt", "+>") or die $!;
my $written = $file->print($wfh);
is($written, length "abc\n\nzyw\n");

my $buf;
$wfh->read($buf, 1000);

is($buf, "abc\n\nzyw\n");

my $wh = IO::File->new("t/outsub.txt", "+>") or die $!;
$written = $file->print($wh, 3, 2);
is($written, 3);

# write bindata
$file = $grid->find_one({'_id' => $save_id});
$wfh = IO::File->new('t/output.png', '+>') or die $!;
$written = $file->print($wfh);
is($written, $file->info->{'length'}, 'bin file length');

#all
my @list = $grid->all;
is(@list, 3, "three files");
for (my $i=0; $i<3; $i++) {
    isa_ok($list[$i], 'MongoDB::GridFS::File');
}
is($list[0]->info->{'length'}, 9, 'checking lens');
is($list[1]->info->{'length'}, 1292706);
is($list[2]->info->{'length'}, 9);

#remove
is($grid->files->query({"_id" => 1})->has_next, 1, 'pre-remove');
is($grid->chunks->query({"files_id" => 1})->has_next, 1);
$file = $grid->remove({"_id" => 1});
is(int($grid->files->query({"_id" => 1})->has_next), 0, 'post-remove');
is(int($grid->chunks->query({"files_id" => 1})->has_next), 0);

