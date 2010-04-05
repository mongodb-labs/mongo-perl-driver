use strict;
use warnings;
use MongoDB;
use boolean;
use Data::Dumper;
use MongoDB::OID;
use Devel::Peek;
use Data::Dump;

my $m = MongoDB::Connection->new(left_host => "localhost", left_port => 27017, 
                                 right_host => "localhost", right_port => 27018);

my $db = $m->get_database("foo");
my $c = $db->get_collection("bar");

while (true) {
  print "finding...";
  eval {
    $c->find_one();
  };
  if ($@) {
    print $@;
  }
  print "\n";
  sleep 1;
}
