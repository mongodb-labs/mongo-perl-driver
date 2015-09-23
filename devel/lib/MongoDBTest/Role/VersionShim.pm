#  Copyright 2009-2014 MongoDB, Inc.
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

use 5.010;
use strict;
use warnings;

package MongoDBTest::Role::VersionShim;

use version;
use MongoDB;

use Moo::Role;
use namespace::clean;

sub do_cmd_on_db {
    my ($self, $client, $database, $cmd) = @_;

    my $db = $client->get_database($database);

    if ( eval { MongoDB->VERSION("v1.0.0") } ) {
        return $db->run_command($cmd);
    }
    else {
        return $db->_try_run_command($cmd);
    }
}

1;
