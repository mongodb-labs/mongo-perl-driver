#  Copyright 2015 - present MongoDB, Inc.
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

use strict;
use warnings;
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::More 0.88;
use Test::Deep;
use Test::Fatal;
use Safe::Isa;

use lib "t/lib";

use MongoDBTest qw/
    build_client
    get_test_db
    clear_testdbs
    server_version
    skip_unless_mongod
/;
use MongoDB::ReadConcern;
use MongoDB::WriteConcern;

my $read_conn_spec = path(
"t/data/read-write-concern/connection-string/"
)->child("read-concern.json");
my $write_conn_spec = $read_conn_spec->sibling("write-concern.json");
my $read_doc_spec = $read_conn_spec->parent(2)->child("/document/read-concern.json");
my $write_doc_spec = $read_doc_spec->sibling("write-concern.json");

my $plan_rc_conn = _json_parse($read_conn_spec);
my $plan_wc_conn = _json_parse($write_conn_spec);
my $plan_rc_doc = _json_parse($read_doc_spec);
my $plan_wc_doc = _json_parse($write_doc_spec);

subtest "$read_conn_spec connection-string" => sub {
    for my $test ( @{ $plan_rc_conn->{tests} } ) {
        my $rc_level = $test->{readConcern}->{level};
        my $uri = $test->{uri};
        my $description = $test->{description};
        my $conn = build_client( host => $uri );
        is_deeply(
            $conn->read_concern->{level},
            $rc_level,
            "read_concern $description ok"
        );
    }
};

subtest "$read_doc_spec document" => sub {
    for my $test ( @{ $plan_rc_doc->{tests} } ) {
        my $rc_level = $test->{readConcern}->{level};
        my $rc_level_doc = $test->{readConcernDocument}->{level};
        my $description = $test->{description};
        my $rc_obj = MongoDB::ReadConcern->new( $test->{readConcern} )->as_args->[1];
        is_deeply(
            $rc_obj,
            defined $rc_obj ? $test->{readConcernDocument} : undef,
            "read_concern $description ok"
        );
    }
};

subtest "$write_conn_spec connection-string" => sub {
    for my $test ( @{ $plan_wc_conn->{tests} } ) {
        my $uri = $test->{uri};
        my $wc_valid = $test->{valid};
        if ( defined $test->{writeConcern}->{wtimeoutMS} ) {
            $test->{writeConcern}->{wtimeout} = delete $test->{writeConcern}->{wtimeoutMS};
        }
        if ( defined $test->{writeConcern}->{journal} ) {
            $test->{writeConcern}->{j} = bool(delete $test->{writeConcern}->{journal})|obj_isa('boolean');
        }
        my $description = $test->{description};

        subtest $description => sub {
            my (@warnings, $conn);
            local $SIG{__WARN__} = sub { push @warnings, $_[0] };
            # wtimeout will be 1000 if we do not send this
            eval { $conn = MongoDB->connect($uri, {wtimeout => undef}) };
            my $error = $@;
            if ( $wc_valid ) {
                cmp_deeply(
                    $conn->write_concern->as_args->[1] || {},
                    $test->{writeConcern},
                    "write_concern ok"
                );
            } else {
                ok( scalar(@warnings) || $error, "should throw or warn" );
            }
        }
    }
};

subtest "$write_doc_spec document" => sub {
    for my $test ( @{ $plan_wc_doc->{tests} } ) {
        my $wc_valid = $test->{valid};
        $test->{writeConcern}->{wtimeout} = delete $test->{writeConcern}->{wtimeoutMS};
        if ( defined $test->{writeConcern}->{journal} ) {
            $test->{writeConcern}->{j} = delete( $test->{writeConcern}->{journal}) ? 1 : 0;
        }
        if ( defined $test->{writeConcernDocument}->{j} ) {
            $test->{writeConcernDocument}->{j} = bool($test->{writeConcernDocument}->{j})|obj_isa('boolean');
        }
        my $description = $test->{description};

        subtest $description => sub {
            my $wc_obj;
            eval { $wc_obj = MongoDB::WriteConcern->new( $test->{writeConcern} ) };
            my $error = $@;
            if ( $wc_valid ) {
              cmp_deeply(
                  $wc_obj->as_args->[1],
                  defined $wc_obj->as_args->[1] ?
                  $test->{writeConcernDocument} : undef,
                  "write_concern ok"
              );
            } else {
              isa_ok(
                  $error,
                  "MongoDB::UsageError"
              );
            }
        }
    }
};

sub _json_parse {
    my $path = shift;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    return $plan;
}

clear_testdbs;

done_testing;
