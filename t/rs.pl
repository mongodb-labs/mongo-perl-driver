use strict;
use warnings;
use MongoDB;
use boolean;
use Data::Dumper;
use MongoDB::OID;
use Devel::Peek;
use Data::Dump;

my $m = MongoDB::Connection::->new(host => "mongodb://localhost:27018", find_master => 1, ssl => $ENV{MONGO_SSL});

my $db = $m->get_database("admin");
my $c = $db->get_collection("bar");

while (true) {
#   print "finding...";
   eval {
       $c->find_one();
   };
   if ($@) {
       print $@;
   }
   else {
       if ($m->_master){
           print "connected to: ".$m->_master->{host}."\n";
       }
       else {
           print "no master\n";
       }
   }
   sleep 1;
}
