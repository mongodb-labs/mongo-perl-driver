use strict;
use warnings;
use Test::More 'no_plan';
use Test::Exception;
use IO::File;
use Tie::IxHash;

use MongoDB;
use MongoDB::GridFS;

use Data::Dumper;

my $m = MongoDB::Connection->new;
my $db = $m->get_database('foo');
my $grid = $db->get_gridfs;
$grid->files->drop;
$grid->chunks->drop;

# test ctor prefix
is('foo.fs.files', $grid->files->full_name);
is('foo.fs.chunks', $grid->chunks->full_name);

my $fancy_grid = $db->get_gridfs("bar");
is('foo.bar.files', $fancy_grid->files->full_name);
is('foo.bar.chunks', $fancy_grid->chunks->full_name);

# test text insert
my $dumb_str = "abc\n\nzyw\n";
my $text_doc = new IO::File("t/input.txt", "r") or die $!;
my $id = $grid->insert($text_doc);
$text_doc->close;

my $chunk = $grid->chunks->find_one();
is(0, $chunk->{'n'});
is("$id", $chunk->{'files_id'}."");
is($dumb_str, $chunk->{'data'});

my $md5 = $db->run_command({"filemd5" => $chunk->{'files_id'}, "root" => "foo.fs.files"});
my $file = $grid->files->find_one();
is($file->{'md5'}, $md5->{'md5'});
is($file->{'length'}, length $dumb_str);
is($chunk->{'files_id'}, $file->{'_id'});

# test bin insert
my $img = new IO::File("t/img.png", "r") or die $!;
$id = $grid->insert($img);
$img->read($dumb_str, 4000000);
$img->close;

$chunk = $grid->chunks->find_one({'files_id' => $id});
is(0, $chunk->{'n'});
is("$id", $chunk->{'files_id'}."");
my $len = 1048576;
is(substr($dumb_str, 0, $len), substr($chunk->{'data'}, 0, $len));

my $file = $grid->files->find_one({'_id' => $id});
is($file->{'length'}, length $dumb_str);
is($chunk->{'files_id'}, $file->{'_id'});

# test inserting metadata
#find_one
#all
#remove
#write

