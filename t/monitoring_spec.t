#
#  Copyright 2018 - present MongoDB, Inc.
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
use Test::More 0.96;
use JSON::MaybeXS;
use Test::Deep;
use Path::Tiny;
use Try::Tiny;
use version;

use MongoDB;

use lib "t/lib";
use MongoDBTest qw/
    skip_unless_mongod
    build_client
    get_test_db
    server_version
    server_type
    get_feature_compat_version
/;

skip_unless_mongod();

#--------------------------------------------------------------------------#
# Event callback for testing -- just closures over an array
#--------------------------------------------------------------------------#

my @events;

sub clear_events { @events = () }
sub event_count { scalar @events }
sub event_cb { push @events, $_[0] }

#--------------------------------------------------------------------------#

my $conn           = build_client( monitoring_callback => \&event_cb );
my $testdb         = get_test_db($conn);
my $server_version = server_version($conn);
my $server_type    = server_type($conn);
my $feat_compat_ver = get_feature_compat_version($conn);
my $coll           = $testdb->get_collection('test_collection');

my $dir = path("t/data/command-monitoring");
my $iterator = $dir->iterator( { recurse => 1 } );
while ( my $path = $iterator->() ) {
    next unless -f $path && $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }

    my $name = $path->relative($dir)->basename(".json");

    subtest $name => sub {
        for my $test ( @{ $plan->{tests} } ) {
            $coll->drop;
            $coll->insert_many( $plan->{data} );
            clear_events();
            my $op   = $test->{operation};
            my $meth = $op->{name};
            $meth =~ s{([A-Z])}{_\L$1}g;
            my $test_meth = "test_$meth";
            plan skip_all => "not implemented"
                unless main->can("$test_meth");
            my $res = main->$test_meth( $test->{description}, $meth, $op->{arguments},
                $test->{expectations} );
        }
    };
}

#--------------------------------------------------------------------------#
# generic tests
#--------------------------------------------------------------------------#

sub test_find {
    my ( $class, $label, $method, $args, $events ) = @_;
    my $filter = delete $args->{filter};
    my $res = $coll->$method( grep { defined } $filter, $args );
    check_event_expectations( $label, $method, $events );
}

sub check_event_expectations {
    my ($label, $method, $expected) = @_;
    my @got = @events;
    my $ok = 1;
    for my $exp ( @$expected ) {
        if (!@got ) {
            $ok = 0;
            last;
        }
        if ( $got[0]->{type} ne $exp->{type} ) {
            shift @got;
            redo;
        }
        ...;
    }
}

sub _prep_to_ignore_special_data {
    my ($hr) = @_;
    if (exists $hr->{command} && exists $hr->{command}{cursor} ) {
        $hr->{command}{cursor}{id} = ignore();
    }
    if (exists $hr->{reply} && exists $hr->{reply}{cursor} ) {
        $hr->{reply}{cursor}{id} = ignore();
    }
}

done_testing;
