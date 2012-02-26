use strict;
use utf8; 
use warnings;
use MongoDB;
use MongoDB::Code;
use DateTime;
use IO::File;
use boolean;
use Data::Dumper;
use MongoDB::OID;
use Devel::Peek;
use File::Copy;
use Data::Dump;
use File::Temp;
use File::Slurp;
use Tie::IxHash;
use FileHandle;

my $conn = MongoDB::Connection::->new("username" => "kristina", "password" => "foo", "db_name" => "bar", "ssl" => $ENV{MONGO_SSL});

my $db = $conn->get_database("bar");
my $c = $db->get_collection("x");

my $count = 0;
while ($count < 10) {
  print "inserting $count...\n";
  eval {
    my $cursor = $c->insert({"name" => 1}, {"safe" => true});
  };

  if ($@) {
    print $@."\n";
  }
  sleep(3);
  $count++;
}
