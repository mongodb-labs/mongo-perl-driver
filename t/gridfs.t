use strict;
use warnings;
use Test::More;
use Test::Exception;
use IO::File;
use File::Temp;
use File::Slurp qw(read_file write_file);
use MongoDB::Timestamp; # needed if db is being run as master

use MongoDB;
use MongoDB::GridFS;
use MongoDB::GridFS::File;
use DateTime;
use FileHandle;

my $m;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $m = MongoDB::MongoClient->new(host => $host, ssl => $ENV{MONGO_SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 62;
}

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
my $ts = DateTime->now;
my $id = $grid->insert($text_doc);
$text_doc->close;

my $chunk = $grid->chunks->find_one();
is(0, $chunk->{'n'});
is("$id", $chunk->{'files_id'}."", "compare returned id");
is($dumb_str, $chunk->{'data'}, "compare file content");

my $md5 = $db->run_command(["filemd5" => $chunk->{'files_id'}, "root" => "fs"]);
my $file = $grid->files->find_one();
ok($file->{'md5'} ne 'd41d8cd98f00b204e9800998ecf8427e', $file->{'md5'});
is($file->{'md5'}, $md5->{'md5'}, $md5->{'md5'});
ok($file->{'uploadDate'}->epoch - $ts->epoch < 10);
is($file->{'chunkSize'}, $MongoDB::GridFS::chunk_size);
is($file->{'length'}, length $dumb_str, "compare file len");
is($chunk->{'files_id'}, $file->{'_id'}, "compare ids");

# test bin insert
my $img = new IO::File("t/img.png", "r") or die $!;
# Windows is dumb
binmode($img);
$id = $grid->insert($img);
my $save_id = $id;
$img->read($dumb_str, 4000000);
$img->close;
my $meta = $grid->files->find_one({'_id' => $save_id});
is($meta->{'length'}, 1292706);

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
$wfh->close();

# slurp
is($file->slurp,"abc\n\nzyw\n",'slurp');

my $buf;
$wfh = IO::File->new("t/output.txt", "<") or die $!;
$wfh->read($buf, 1000);
#$wfh->read($buf, length( "abc\n\nzyw\n"));

is($buf, "abc\n\nzyw\n", "read chars from tmpfile");

my $wh = IO::File->new("t/outsub.txt", "+>") or die $!;
$written = $file->print($wh, 3, 2);
is($written, 3);

# write bindata
$file = $grid->find_one({'_id' => $save_id});
$wfh = IO::File->new('t/output.png', '+>') or die $!;
$wfh->binmode;
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

# remove
is($grid->files->query({"_id" => 1})->has_next, 1, 'pre-remove');
is($grid->chunks->query({"files_id" => 1})->has_next, 1);
$file = $grid->remove({"_id" => 1});
is(int($grid->files->query({"_id" => 1})->has_next), 0, 'post-remove');
is(int($grid->chunks->query({"files_id" => 1})->has_next), 0);

# remove just_one
$grid->drop;
$img = new IO::File("t/img.png", "r") or die $!;
$grid->insert($img, {"filename" => "garbage.png"});
$grid->insert($img, {"filename" => "garbage.png"});

is($grid->files->count, 2);
$grid->remove({'filename' => 'garbage.png'}, 1);
is($grid->files->count, 1, 'remove just one');

unlink 't/output.txt', 't/output.png', 't/outsub.txt';

# multi-chunk
{
    $grid->drop;

    foreach (1..3) {
        my $txt = "HELLO" x 1_000_000; # 5MB
        
        my $fh = File::Temp->new;
        write_file( $fh->filename, $txt ) || die $!;
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

    my $file = $grid->find_one;
    is($file->info->{filename}, 'hello.txt');
    is($file->info->{length}, 5);
}

# safe insert
{
    $grid->drop;
    $img = new IO::File("t/img.png", "r") or die $!;
    $img->binmode;
    $grid->insert($img, {filename => 'img.png'}, {safe => boolean::true});

    my $file = $grid->find_one;
    is($file->info->{filename}, 'img.png', 'safe insert');
    is($file->info->{length}, 1292706);
    ok($file->info->{md5} ne 'd41d8cd98f00b204e9800998ecf8427e', $file->info->{'md5'});
}

# get, put, delete
{
    $grid->drop;

    $img = new IO::File("t/img.png", "r") or die $!;
    $img->binmode;

    my $id = $grid->put($img, {_id => 1, filename => 'img.png'});
    is($id, 1, "put _id");

    $id = $grid->put($img);
    isa_ok($id, 'MongoDB::OID');

    eval {
        $id = $grid->put($img, {_id => 1, filename => 'img.png'});
    };

    ok($@ and $@ =~ /^E11000/, 'duplicate key exception');

    my $file = $grid->get(1);
    is($file->info->{filename}, 'img.png');
    ok($file->info->{md5} ne 'd41d8cd98f00b204e9800998ecf8427e', $file->info->{'md5'});

    $grid->delete(1);

    my $coll = $db->get_collection('fs.files');

    $file = $coll->find_one({_id => 1});
    is($file, undef);

    $coll = $db->get_collection('fs.chunks');
    $file = $coll->find_one({files_id => 1});
    is($file, undef);
}


END {
    if ($db) {
        $db->drop;
    }
}
