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

my $conn = MongoDB::MongoClient->new("username" => "kristina", "password" => "foo", "db_name" => "bar", "ssl" => $ENV{MONGO_SSL});

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
