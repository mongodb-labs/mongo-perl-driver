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
use MongoDB;
use boolean;
use Data::Dumper;
use MongoDB::OID;
use Devel::Peek;
use Data::Dump;

my $m = MongoDB::MongoClient->new(host => "mongodb://localhost:27018", find_master => 1, ssl => $ENV{MONGO_SSL});

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
