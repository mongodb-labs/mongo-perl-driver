use strict;
use warnings;
use Test::More tests => 22;
use Test::Exception;
use IO::File;
use Tie::IxHash;

use MongoDB;
use MongoDB::GridFS;
use MongoDB::GridFS::File;

use Data::Dumper;

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
my $wfh = IO::File->new("t/temp.txt", "+>") or die $!;
$file->print($wfh);
$wfh->setpos(0);
$wfh->read(my $buf, 1000);

#is($buf, "abc\n\nzyw\n");



#all
#my @list = $grid->all;
#print Dumper(@list);

#remove
#write

