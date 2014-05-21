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
use utf8;
use Test::More 0.88;
use Test::Fatal;
use Test::Deep qw/!blessed/;
use Scalar::Util qw/refaddr/;
use Syntax::Keyword::Junction qw/any/;
use Tie::IxHash;
use boolean;

use MongoDB;
use MongoDB::Error;

use lib "t/lib";
use MongoDBTest '$conn', '$testdb', '$using_2_6';

my $coll = $testdb->get_collection("test_collection");

my $ismaster      = $testdb->run_command( { ismaster     => 1 } );
my $server_status = $testdb->run_command( { serverStatus => 1 } );

subtest "constructors" => sub {
    my @constructors = qw(
      initialize_ordered_bulk_op initialize_unordered_bulk_op
      ordered_bulk unordered_bulk
    );
    for my $method (@constructors) {
        my $bulk = $coll->$method;
        isa_ok( $bulk, 'MongoDB::BulkWrite', $method );
        if ( $method =~ /unordered/ ) {
            ok( !$bulk->ordered, "ordered attr is false" );
        }
        else {
            ok( $bulk->ordered, "ordered attr is true" );
        }
        is(
            refaddr $bulk->collection,
            refaddr $coll,
            "MongoDB::BulkWrite holds ref to originating Collection"
        );
    }
};

note("QA-477 INSERT");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: insert errors" => sub {
        my $bulk = $coll->$method;
        # raise errors on wrong arg types
        my %bad_args = (
            SCALAR => ['foo'],
            LIST   => [ {}, {} ],
            EMPTY  => [],
        );

        for my $k ( sort keys %bad_args ) {
            like(
                exception { $bulk->insert( @{ $bad_args{$k} } ) },
                qr/argument to insert must be a single hashref, arrayref or Tie::IxHash/,
                "insert( $k ) throws an error"
            );
        }

        like(
            exception { $bulk->insert( ['foo'] ) },
            qr{array reference to insert must have key/value pairs},
            "insert( ['foo'] ) throws an error",
        );

        like(
            exception { $bulk->find( {} )->insert( {} ) },
            qr/^Can't locate object method "insert"/,
            "find({})->insert({}) throws an error",
        );

        is( exception { $bulk->insert( { '$key' => 1 } ) },
            undef, "queuing insertion of document with \$key is allowed" );

        my $err = exception { $bulk->execute };
        isa_ok( $err, 'MongoDB::WriteError', "executing insertion with \$key" );

        is( $err->message, "writeErrors: 1", "WriteError message" );
        like( $err->result->last_errmsg, qr/\$key/, "WriteError details mentions \$key" );
    };

    subtest "$method: successful insert" => sub {
        $coll->drop;
        my $bulk = $coll->$method;
        is( $coll->count, 0, "no docs in collection" );
        $bulk->insert( { _id => 1 } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on insert" ) or diag explain $err;
        is( $coll->count, 1, "one doc in collection" );
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        is_deeply(
            $result,
            MongoDB::WriteResult->new(
                nInserted => 1,
                nModified => ( $using_2_6 ? 0 : undef ),
                op_count  => 1,
                batch_count => 1,
            ),
            "result object correct"
        );
    };

    subtest "$method insert without _id" => sub {
        $coll->drop;
        my $bulk = $coll->$method;
        is( $coll->count, 0, "no docs in collection" );
        $bulk->insert( {} );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on insert" ) or diag explain $err;
        is( $coll->count, 1, "one doc in collection" );
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        is_deeply(
            $result,
            MongoDB::WriteResult->new(
                nInserted => 1,
                nModified => ( $using_2_6 ? 0 : undef ),
                op_count  => 1,
                batch_count => 1,
            ),
            "result object correct"
        );
        my $id = $coll->find_one()->{_id};
        # OID PIDs are the low 16 bits
        is( $id->_get_pid, $$ & 0xffff, "generated ID has our PID" )
          or diag sprintf( "got OID: %s but our PID is %x", $id->value, $$ );
    };

}

note("QA-477 FIND");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: find" => sub {
        my $bulk = $coll->$method;
        like(
            exception { $bulk->find },
            qr/find requires a criteria document/,
            "find without doc selector throws exception"
        );
    };
}

note("QA-477 UPDATE and UPDATE_ONE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "update and update_one errors with $method" => sub {
        my $bulk = $coll->$method;
        # raise errors on wrong arg types
        my %bad_args = (
            SCALAR => ['foo'],
            EMPTY  => [],     # not in QA test
        );

        for my $update (qw/update update_one/) {
            for my $k ( sort keys %bad_args ) {
                like(
                    exception { $bulk->find( {} )->$update( @{ $bad_args{$k} } ) },
                    qr/argument to $update must be a single hashref, arrayref or Tie::IxHash/,
                    "$update( $k ) throws an error"
                );
            }

            like(
                exception { $bulk->$update( { '$set' => { x => 1 } } ) },
                qr/^Can't locate object method "$update"/,
                "$update on bulk object (without find) throws an error",
            );

            like(
                exception { $bulk->find( {} )->$update( { key => 1 } ) },
                qr/$update document can't have non- '\$' prefixed field names: key/,
                "single non-op key in $update doc throws exception"
            );

            like(
                exception { $bulk->find( {} )->$update( { key => 1, '$key' => 1 } ) },
                qr/$update document can't have non- '\$' prefixed field names: key/,
                "mixed op and non-op key in $update doc throws exception"
            );

        }
    };

    subtest "update all docs with $method" => sub {
        $coll->drop;
        my $bulk = $coll->$method;
        $coll->insert($_) for map { { key => $_ } } 1, 2;
        my @docs = $coll->find( {} )->all;

        $bulk->find( {} )->update( { '$set' => { x => 3 } } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 2,
                nModified   => ( $using_2_6 ? 2 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        );

        # check expected values
        $_->{x} = 3 for @docs;
        cmp_deeply( [ $coll->find( {} )->all ], \@docs, "all documents updated" );
    };

    subtest "update only matching docs with $method" => sub {
        $coll->drop;
        my $bulk = $coll->$method;
        $coll->insert($_) for map { { key => $_ } } 1, 2;
        my @docs = $coll->find( {} )->all;

        $bulk->find( { key => 1 } )->update( { '$set' => { x => 1 } } );
        $bulk->find( { key => 2 } )->update( { '$set' => { x => 2 } } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        is_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 2,
                nModified   => ( $using_2_6 ? 2 : undef ),
                op_count    => 2,
                batch_count => $using_2_6 ? 1 : 2,
            ),
            "result object correct"
        );

        # check expected values
        $_->{x} = $_->{key} for @docs;
        cmp_deeply( [ $coll->find( {} )->all ], \@docs, "all documents updated" );
    };

    subtest "update_one with $method" => sub {
        $coll->drop;
        my $bulk = $coll->$method;
        $coll->insert($_) for map { { key => $_ } } 1, 2;

        $bulk->find( {} )->update_one( { '$set' => { key => 3 } } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        is_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 1,
                nModified   => ( $using_2_6 ? 1 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        );

        # check expected values
        is( $coll->find( { key => 3 } )->count, 1, "one document updated" );
    };

}

note("QA-477 REPLACE_ONE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "replace_one errors with $method" => sub {
        my $bulk = $coll->$method;
        # raise errors on wrong arg types
        my %bad_args = (
            SCALAR => ['foo'],
            EMPTY  => [],     # not in QA test
        );

        for my $k ( sort keys %bad_args ) {
            like(
                exception { $bulk->find( {} )->replace_one( @{ $bad_args{$k} } ) },
                qr/argument to replace_one must be a single hashref, arrayref or Tie::IxHash/,
                "replace_one( $k ) throws an error"
            );
        }

        like(
            exception { $bulk->replace_one( { '$set' => { x => 1 } } ) },
            qr/^Can't locate object method "replace_one"/,
            "replace_one on bulk object (without find) throws an error",
        );

        like(
            exception { $bulk->find( {} )->replace_one( { '$key' => 1 } ) },
            qr/replace_one document can't have '\$' prefixed field names: \$key/,
            "single op key in replace_one doc throws exception"
        );

        like(
            exception { $bulk->find( {} )->replace_one( { key => 1, '$key' => 1 } ) },
            qr/replace_one document can't have '\$' prefixed field names: \$key/,
            "mixed op and non-op key in replace_one doc throws exception"
        );

    };

    subtest "replace_one with $method" => sub {
        $coll->drop;
        my $bulk = $coll->$method;
        $coll->insert( { key => 1 } ) for 1 .. 2;

        $bulk->find( {} )->replace_one( { key => 3 } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        is_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 1,
                nModified   => ( $using_2_6 ? 1 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        );

        # check expected values
        my $distinct =
          $testdb->run_command( [ distinct => $coll->name, key => "key" ] )->{values};
        cmp_deeply( $distinct, bag( 1, 3 ), "only one document replaced" );
    };

}

note("QA-477 UPSERT-UPDATE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "upsert errors with $method" => sub {
        my $bulk = $coll->$method;

        like(
            exception { $bulk->upsert() },
            qr/^Can't locate object method "upsert"/,
            "upsert on bulk object (without find) throws an error",
        );

        like(
            exception { $bulk->find( {} )->upsert( {} ) },
            qr/the upsert method takes no arguments/,
            "upsert( NONEMPTY ) throws an error"
        );

    };

    subtest "upsert-update insertion with $method" => sub {
        $coll->drop;
        my $bulk = $coll->$method;

        $bulk->find( { key => 1 } )->update( { '$set' => { x => 1 } } );
        $bulk->find( { key => 2 } )->upsert->update( { '$set' => { x => 2 } } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nUpserted => 1,
                nModified => ( $using_2_6 ? 0 : undef ),
                upserted  => [ { index => 1, _id => ignore() } ],
                op_count  => 2,
                batch_count => $using_2_6 ? 1 : 2,
            ),
            "result object correct"
        ) or diag explain $result;

        cmp_deeply(
            [ $coll->find( {} )->all ],
            [ { _id => ignore(), key => 2, x => 2 } ],
            "upserted document correct"
        );

        $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->update( { '$set' => { x => 1 } } );
        $bulk->find( { key => 2 } )->upsert->update( { '$set' => { x => 2 } } );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on second upsert-update" ) or diag explain $err;
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 1,
                nModified   => ( $using_2_6 ? 0 : undef ),
                op_count    => 2,
                batch_count => $using_2_6 ? 1 : 2,
            ),
            "result object correct"
        ) or diag explain $result;
    };

    subtest "upsert-update updates with $method" => sub {
        $coll->drop;
        $coll->insert( { key => 1 } ) for 1 .. 2;
        my @docs = $coll->find( {} )->all;

        my $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->upsert->update( { '$set' => { x => 1 } } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 2,
                nModified   => ( $using_2_6 ? 2 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;

        $_->{x} = 1 for @docs;
        cmp_deeply( [ $coll->find( {} )->all ], \@docs, "all documents updated" );
    };

    subtest "upsert-update large doc with $method" => sub {
        $coll->drop;

        # QA test says big_string should be 16MiB - 31 long, but { _id => $oid,
        # key => 1, x => $big_string } exceeds 16MiB when BSON encoded unless
        # the bigstring is 16MiB - 41.  This may be a peculiarity of Perl's
        # BSON type encoding.
        #
        # Using legacy API, the bigstring must be 16MiB - 97 for some reason.

        my $big_string = "a" x ( 16 * 1024 * 1024 - $using_2_6 ? 41 : 97 );

        my $bulk = $coll->$method;
        $bulk->find( { key => "1" } )->upsert->update( { '$set' => { x => $big_string } } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-update" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nUpserted   => 1,
                nModified   => ( $using_2_6 ? 0 : undef ),
                upserted    => [ { index => 0, _id => ignore() } ],
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;
    };

}

note("QA-477 UPSERT-UPDATE_ONE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "upsert-update_one insertion with $method" => sub {
        $coll->drop;

        my $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->update_one( { '$set' => { x => 1 } } ); # not upsert
        $bulk->find( { key => 2 } )->upsert->update_one( { '$set' => { x => 2 } } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-update_one" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nUpserted => 1,
                nModified => ( $using_2_6 ? 0 : undef ),
                upserted  => [ { index => 1, _id => ignore() } ],
                op_count  => 2,
                batch_count => $using_2_6 ? 1 : 2,
            ),
            "result object correct"
        ) or diag explain $result;

        cmp_deeply(
            [ $coll->find( {} )->all ],
            [ { _id => ignore(), key => 2, x => 2 } ],
            "upserted document correct"
        );

    };

    subtest "upsert-update_one (no insert) with $method" => sub {
        $coll->drop;
        $coll->insert( { key => 1 } ) for 1 .. 2;
        my @docs = $coll->find( {} )->all;

        my $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->upsert->update_one( { '$set' => { x => 2 } } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-update_one" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 1,
                nModified   => ( $using_2_6 ? 1 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;

        # add expected key to one document only
        $docs[0]{x} = 2;
        my @got = $coll->find( {} )->all;

        cmp_deeply( \@got, bag(@docs), "updated document correct" )
          or diag explain \@got;

    };
}

note("QA-477 UPSERT-REPLACE_ONE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "upsert-replace_one insertion with $method" => sub {
        $coll->drop;

        my $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->replace_one( { x => 1 } ); # not upsert
        $bulk->find( { key => 2 } )->upsert->replace_one( { x => 2 } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-replace_one" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nUpserted => 1,
                nModified => ( $using_2_6 ? 0 : undef ),
                upserted  => [ { index => 1, _id => ignore() } ],
                op_count  => 2,
                batch_count => $using_2_6 ? 1 : 2,
            ),
            "result object correct"
        ) or diag explain $result;

        cmp_deeply(
            [ $coll->find( {} )->all ],
            [ { _id => ignore(), x => 2 } ],
            "upserted document correct"
        );

    };

    subtest "upsert-replace_one (no insert) with $method" => sub {
        $coll->drop;
        $coll->insert( { key => 1 } ) for 1 .. 2;
        my @docs = $coll->find( {} )->all;

        my $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->upsert->replace_one( { x => 2 } );

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on upsert-replace_one" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nMatched    => 1,
                nModified   => ( $using_2_6 ? 1 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;

        # change one expected doc only
        $docs[0]{x} = 2;
        delete $docs[0]{key};

        my @got = $coll->find( {} )->all;

        cmp_deeply( \@got, bag(@docs), "updated document correct" )
          or diag explain \@got;

    };
}

note("QA-477 REMOVE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "remove errors with $method" => sub {
        my $bulk = $coll->$method;

        like(
            exception { $bulk->remove() },
            qr/^Can't locate object method "remove"/,
            "remove on bulk object (without find) throws an error",
        );
    };

    subtest "remove all with $method" => sub {
        $coll->drop;
        $coll->insert( { key => 1 } ) for 1 .. 2;

        my $bulk = $coll->$method;
        $bulk->find( {} )->remove;

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on remove" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nRemoved    => 2,
                nModified   => ( $using_2_6 ? 0 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;

        is( $coll->count, 0, "all documents removed" );
    };

    subtest "remove matching with $method" => sub {
        $coll->drop;
        $coll->insert( { key => $_ } ) for 1 .. 2;

        my $bulk = $coll->$method;
        $bulk->find( { key => 1 } )->remove;

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on remove" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nRemoved    => 1,
                nModified   => ( $using_2_6 ? 0 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;

        cmp_deeply(
            [ $coll->find( {} )->all ],
            [ { _id => ignore(), key => 2 } ],
            "correct object remains"
        );
    };
}

note("QA-477 REMOVE_ONE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "remove_one errors with $method" => sub {
        my $bulk = $coll->$method;

        like(
            exception { $bulk->remove_one() },
            qr/^Can't locate object method "remove_one"/,
            "remove_one on bulk object (without find) throws an error",
        );
    };

    subtest "remove_one with $method" => sub {
        $coll->drop;
        $coll->insert( { key => 1 } ) for 1 .. 2;

        my $bulk = $coll->$method;
        $bulk->find( {} )->remove_one;

        my ( $result, $err );
        $err = exception { $result = $bulk->execute };
        is( $err, undef, "no error on remove_one" ) or diag explain $err;
        isa_ok( $result, 'MongoDB::WriteResult', "result object" );
        cmp_deeply(
            $result,
            MongoDB::WriteResult->new(
                nRemoved    => 1,
                nModified   => ( $using_2_6 ? 0 : undef ),
                op_count    => 1,
                batch_count => 1,
            ),
            "result object correct"
        ) or diag explain $result;

        is( $coll->count, 1, "only one doc removed" );
    };
}

note("QA-477 MIXED OPERATIONS, UNORDERED");
subtest "mixed operations, unordered" => sub {
    $coll->drop;
    $coll->insert( { a => $_ } ) for 1 .. 2;

    my $bulk = $coll->initialize_unordered_bulk_op;
    $bulk->find( { a => 1 } )->update( { '$set' => { b => 1 } } );
    $bulk->find( { a => 2 } )->remove;
    $bulk->insert( { a => 3 } );
    $bulk->find( { a => 4 } )->upsert->update_one( { '$set' => { b => 4 } } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on mixed operations" ) or diag explain $err;
    cmp_deeply(
        $result,
        MongoDB::WriteResult->new(
            nInserted   => 1,
            nMatched    => 1,
            nModified   => ( $using_2_6 ? 1 : undef ),
            nUpserted   => 1,
            nRemoved    => 1,
            op_count    => 4,
            batch_count => $using_2_6 ? 3 : 4,
            # XXX QA Test says index should be 3, but with unordered, that's
            # not guaranteed, so we ignore the value
            upserted => [ { index => ignore(), _id => obj_isa("MongoDB::OID") } ],
        ),
        "result object correct"
    ) or diag explain $result;

};

note("QA-477 MIXED OPERATIONS, ORDERED");
subtest "mixed operations, ordered" => sub {
    $coll->drop;

    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->insert( { a => 1 } );
    $bulk->find( { a => 1 } )->update_one( { '$set' => { b => 1 } } );
    $bulk->find( { a => 2 } )->upsert->update_one( { '$set' => { b => 2 } } );
    $bulk->insert( { a => 3 } );
    $bulk->find( { a => 3 } )->remove;

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on mixed operations" ) or diag explain $err;
    cmp_deeply(
        $result,
        MongoDB::WriteResult->new(
            nInserted   => 2,
            nUpserted   => 1,
            nMatched    => 1,
            nModified   => ( $using_2_6 ? 1 : undef ),
            nRemoved    => 1,
            op_count    => 5,
            batch_count => $using_2_6 ? 4 : 5,
            upserted    => [ { index => 2, _id => obj_isa("MongoDB::OID") } ],
        ),
        "result object correct"
    ) or diag explain $result;

};

note("QA-477 UNORDERED BATCH WITH ERRORS");
subtest "unordered batch with errors" => sub {
    $coll->drop;
    $coll->ensure_index( [ a => 1 ], { unique => 1 } );

    my $bulk = $coll->initialize_unordered_bulk_op;
    $bulk->insert( { b => 1, a => 1 } );
    $bulk->find( { b => 2 } )->upsert->update_one( { '$set' => { a => 1 } } );
    $bulk->find( { b => 3 } )->upsert->update_one( { '$set' => { a => 2 } } );
    $bulk->find( { b => 2 } )->upsert->update_one( { '$set' => { a => 1 } } );
    $bulk->insert( { b => 4, a => 3 } );
    $bulk->insert( { b => 5, a => 1 } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    isa_ok( $err, 'MongoDB::WriteError', 'caught error' );
    my $details = $err->result;

    # Check if all ops ran in two batches (unless we're on a legacy server)
    is( $details->op_count, 6, "op_count" );
    is( $details->batch_count, $using_2_6 ? 2 : 6, "op_count" );

    # XXX QA 477 doesn't cover *both* possible orders.  Either the inserts go
    # first or the upsert/update_ones goes first and different result states
    # are possible for each case.

    if ( $details->nInserted == 2 ) {
        note("inserts went first");
        is( $details->nInserted, 2, "nInserted" );
        is( $details->nUpserted, 1, "nUpserted" );
        is( $details->nRemoved,  0, "nRemoved" );
        is( $details->nMatched,  0, "nMatched" );
        is( $details->nModified, ( $using_2_6 ? 0 : undef ), "nModified" );
        is( $details->count_writeErrors, 3, "writeError count" )
          or diag explain $details;
        cmp_deeply( $details->upserted, [ { index => 4, _id => obj_isa("MongoDB::OID") }, ],
            "upsert list" );
    }
    else {
        note("updates went first");
        is( $details->nInserted, 1, "nInserted" );
        is( $details->nUpserted, 2, "nUpserted" );
        is( $details->nRemoved,  0, "nRemoved" );
        is( $details->nMatched,  1, "nMatched" );
        is( $details->nModified, ( $using_2_6 ? 0 : undef ), "nModified" );
        is( $details->count_writeErrors, 2, "writeError count" )
          or diag explain $details;
        cmp_deeply(
            $details->upserted,
            [
                { index => 0, _id => obj_isa("MongoDB::OID") },
                { index => 1, _id => obj_isa("MongoDB::OID") },
            ],
            "upsert list"
        );
    }

    my $distinct =
      $testdb->run_command( [ distinct => $coll->name, key => "a" ] )->{values};
    cmp_deeply( $distinct, bag( 1 .. 3 ), "distinct keys" );

};

note("QA-477 ORDERED BATCH WITH ERRORS");
subtest "ordered batch with errors" => sub {
    $coll->drop;
    $coll->ensure_index( [ a => 1 ], { unique => 1 } );

    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->insert( { b => 1, a => 1 } );
    $bulk->find( { b => 2 } )->upsert->update_one( { '$set' => { a => 1 } } );
    $bulk->find( { b => 3 } )->upsert->update_one( { '$set' => { a => 2 } } );
    $bulk->find( { b => 2 } )->upsert->update_one( { '$set' => { a => 1 } } ); # fail
    $bulk->insert( { b => 4, a => 3 } );
    $bulk->insert( { b => 5, a => 1 } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    isa_ok( $err, 'MongoDB::WriteError', 'caught error' );
    my $details = $err->result;
    is( $details->nUpserted, 0, "nUpserted" );
    is( $details->nMatched,  0, "nMatched" );
    is( $details->nRemoved,  0, "nRemoved" );
    is( $details->nModified, ( $using_2_6 ? 0 : undef ), "nModified" );
    is( $details->nInserted, 1, "nInserted" );

    # on 2.6+, 4 ops run in two batches; but on legacy, we get an error on
    # the first update_one, so we only have two ops, still in two batches
    is( $details->op_count, $using_2_6 ? 4 : 2, "op_count" );
    is( $details->batch_count, 2, "op_count" );

    is( $details->count_writeErrors,       1,     "writeError count" );
    is( $details->writeErrors->[0]{code},  11000, "error code" );
    is( $details->writeErrors->[0]{index}, 1,     "error index" );
    ok( length $details->writeErrors->[0]{errmsg}, "error string" );

    cmp_deeply(
        $details->writeErrors->[0]{op},
        {
            q => { b      => 2 },
            u => { '$set' => { a => 1 } },
            multi  => 0,
            upsert => 1,
        },
        "error op"
    );

    is( $coll->count, 1, "subsequent inserts did not run" );
};

note("QA-477 BATCH SPLITTING: maxBsonObjectSize");
subtest "ordered batch split on size" => sub {
    $coll->drop;

    my $bulk = $coll->initialize_ordered_bulk_op;
    my $big_string = "a" x ( 4 * 1024 * 1024 );
    $bulk->insert( { _id => $_, a => $big_string } ) for 0 .. 5;
    $bulk->insert( { _id => 0 } );  # will fail
    $bulk->insert( { _id => 100 } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    isa_ok( $err, 'MongoDB::WriteError', 'caught error' )
      or diag $err;
    my $details = $err->result;
    my $errdoc  = $details->writeErrors->[0];
    is( $details->nInserted,         6,     "nInserted" );
    is( $details->count_writeErrors, 1,     "count_writeErrors" );
    is( $errdoc->{code},             11000, "error code" );
    is( $errdoc->{index},            6,     "error index" );
    ok( length( $errdoc->{errmsg} ), "error message" );

    is( $coll->count, 6, "collection count" );
};

subtest "unordered batch split on size" => sub {
    $coll->drop;

    my $bulk = $coll->initialize_unordered_bulk_op;
    my $big_string = "a" x ( 4 * 1024 * 1024 );
    $bulk->insert( { _id => $_, a => $big_string } ) for 0 .. 5;
    $bulk->insert( { _id => 0 } );  # will fail
    $bulk->insert( { _id => 100 } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    isa_ok( $err, 'MongoDB::WriteError', 'caught error' )
      or diag $err;
    my $details = $err->result;
    my $errdoc  = $details->writeErrors->[0];
    is( $details->nInserted,         7,     "nInserted" );
    is( $details->count_writeErrors, 1,     "count_writeErrors" );
    is( $errdoc->{code},             11000, "error code" );
    is( $errdoc->{index},            6,     "error index" );
    ok( length( $errdoc->{errmsg} ), "error message" );

    is( $coll->count, 7, "collection count" );
};

note("QA-477 BATCH SPLITTING: maxWriteBatchSize");
subtest "ordered batch split on number of ops" => sub {
    $coll->drop;

    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->insert( { _id => $_ } ) for 0 .. 1999;
    $bulk->insert( { _id => 0 } );    # will fail
    $bulk->insert( { _id => 10000 } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    isa_ok( $err, 'MongoDB::WriteError', 'caught error' )
      or diag $err;
    my $details = $err->result;
    my $errdoc  = $details->writeErrors->[0];
    is( $details->nInserted,         2000,  "nInserted" );
    is( $details->count_writeErrors, 1,     "count_writeErrors" );
    is( $errdoc->{code},             11000, "error code" );
    is( $errdoc->{index},            2000,  "error index" );
    ok( length( $errdoc->{errmsg} ), "error message" );

    is( $coll->count, 2000, "collection count" );
};

subtest "unordered batch split on number of ops" => sub {
    $coll->drop;

    my $bulk = $coll->initialize_unordered_bulk_op;
    $bulk->insert( { _id => $_ } ) for 0 .. 1999;
    $bulk->insert( { _id => 0 } );    # will fail
    $bulk->insert( { _id => 10000 } );

    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    isa_ok( $err, 'MongoDB::WriteError', 'caught error' )
      or diag $err;
    my $details = $err->result;
    my $errdoc  = $details->writeErrors->[0];
    is( $details->nInserted,         2001,  "nInserted" );
    is( $details->count_writeErrors, 1,     "count_writeErrors" );
    is( $errdoc->{code},             11000, "error code" );
    is( $errdoc->{index},            2000,  "error index" );
    ok( length( $errdoc->{errmsg} ), "error message" );

    is( $coll->count, 2001, "collection count" );
};

note("QA-477 RE-RUNNING A BATCH");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: rerun a bulk operation" => sub {
        $coll->drop;

        my $bulk = $coll->$method;
        $bulk->insert( {} );

        my $err = exception { $bulk->execute };
        is( $err, undef, "first execute succeeds" );

        $err = exception { $bulk->execute };
        isa_ok( $err, 'MongoDB::Error', "re-running a bulk op throws exception" );

        like( $err->message, qr/bulk op execute called more than once/, "error message" )
          or diag explain $err;
    };
}

note("QA-477 EMPTY BATCH");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: empty bulk operation" => sub {
        my $bulk = $coll->$method;

        my $err = exception { $bulk->execute };
        isa_ok( $err, 'MongoDB::Error', "empty bulk op throws exception" );

        like( $err->message, qr/no bulk ops to execute/, "error message" )
          or diag explain $err;
    };
}

note("NO JOURNAL");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: no journal" => sub {
        plan skip_all => 'needs a server without journaling'
          if exists $server_status->{dur};

        $coll->drop;
        my $bulk = $coll->$method;
        $bulk->insert( {} );
        my $err = exception { $bulk->execute( { j => 1 } ) };
        isa_ok( $err, 'MongoDB::DatabaseError', "executing j:1 on nojournal throws error" );
        like( $err->message, qr/journal/, "error message mentions journal" );
    };
}

note("QA-477 W>1 AGAINST STANDALONE");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: w > 1 against standalone (explicit)" => sub {
        plan skip_all => 'needs a standalone server'
          if $server_status->{repl};

        $coll->drop;
        my $bulk = $coll->$method;
        $bulk->insert( {} );
        my $err = exception { $bulk->execute( { w => 2 } ) };
        isa_ok( $err, 'MongoDB::DatabaseError',
            "executing write concern w > 1 throws error" );
        like( $err->message, qr/replica/, "error message mentions replication" );
    };

    subtest "$method: w > 1 against standalone (implicit)" => sub {
        plan skip_all => 'needs a standalone server'
          if $server_status->{repl};

        $coll->drop;
        $conn->w(2);
        my $bulk = $coll->$method;
        $bulk->insert( {} );
        my $err = exception { $bulk->execute() };
        isa_ok( $err, 'MongoDB::DatabaseError',
            "executing write concern w > 1 throws error" );
        like( $err->message, qr/replica/, "error message mentions replication" );
        $conn->w(1);
    };
}

note("QA-477 WTIMEOUT PLUS DUPLICATE KEY ERROR");
subtest "initialize_unordered_bulk_op: wtimeout plus duplicate keys" => sub {
    plan skip_all => 'needs a replica set'
      unless $ismaster->{hosts};

    # asking for w more than N hosts will trigger the error we need
    my $W = @{ $ismaster->{hosts} } + 1;

    $coll->drop;
    my $bulk = $coll->initialize_unordered_bulk_op;
    $bulk->insert( { _id => 1 } );
    $bulk->insert( { _id => 1 } );
    my $err = exception { $bulk->execute( { w => $W, wtimeout => 100 } ) };
    isa_ok( $err, 'MongoDB::WriteError', "executing throws error" );
    my $details = $err->result;
    is( $details->nInserted,                1, "nInserted == 1" );
    is( $details->count_writeErrors,        1, "one write error" );
    is( $details->count_writeConcernErrors, 1, "one write concern error" );
};

note("QA-477 W = 0");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: w = 0" => sub {
        $coll->drop;
        my $bulk = $coll->$method;

        $bulk->insert( { _id => 1 } );
        $bulk->insert( { _id => 1 } );
        $bulk->insert( { _id => 2 } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute( { w => 0 } ) };
        is( $err, undef, "execute with w = 0 doesn't throw error" )
          or diag explain $err;

        my $expect = $method eq 'initialize_ordered_bulk_op' ? 1 : 2;
        is( $coll->count, $expect, "document count ($expect)" );
    };
}

# This test was not included in the QA-477 test plan; it ensures that
# write concerns are applied only after all operations finish
note("WRITE CONCERN ERRORS");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: write concern errors" => sub {
        plan skip_all => 'needs a replica set'
          unless $ismaster->{hosts};

        # asking for w more than N hosts will trigger the error we need
        my $W = @{ $ismaster->{hosts} } + 1;

        $coll->drop;
        my $bulk = $coll->$method;
        $bulk->insert( { _id => 1 } );
        $bulk->insert( { _id => 2 } );
        $bulk->find( { id => 3 } )->upsert->update( { '$set' => { x => 2 } } );
        $bulk->insert( { _id => 4 } );
        my $err = exception { $bulk->execute( { w => $W, wtimeout => 100 } ) };
        isa_ok( $err, 'MongoDB::WriteConcernError', "executing throws error" );
        my $details = $err->result;
        is( $details->nInserted,         3, "nInserted" );
        is( $details->nUpserted,         1, "nUpserted" );
        is( $details->count_writeErrors, 0, "no write errors" );
        ok( $details->count_writeConcernErrors, "got write concern errors" );
    };
}

# Not in QA-477 -- Many methods take hashrefs, arrayrefs or Tie::IxHash
# objects.  The following tests check that arrayrefs and Tie::IxHash are legal
# arguments to find, insert, update, update_one and replace_one.  The
# remove and remove_one methods take no arguments and don't need tests

note("ARRAY REFS"); # Not in QA-477 -- this is perl driver specific
subtest "insert (ARRAY)" => sub {
    $coll->drop;
    my $bulk = $coll->initialize_ordered_bulk_op;
    is( $coll->count, 0, "no docs in collection" );
    $bulk->insert( [ _id => 1 ] );
    $bulk->insert( [] );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on insert" ) or diag explain $err;
    is( $coll->count, 2, "doc count" );
};

subtest "update (ARRAY)" => sub {
    $coll->drop;
    $coll->insert( { _id => 1 } );
    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->find( [] )->update( [ '$set' => { x => 2 } ] );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on update" ) or diag explain $err;
    is( $coll->find_one( {} )->{x}, 2, "document updated" );
};

subtest "update_one (ARRAY)" => sub {
    $coll->drop;
    $coll->insert( { _id => $_ } ) for 1 .. 2;
    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->find( [] )->update_one( [ '$set' => { x => 2 } ] );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on update_one" ) or diag explain $err;
    is( $coll->count( { x => 2 } ), 1, "only one doc updated" );
};

subtest "replace_one (ARRAY)" => sub {
    $coll->drop;
    $coll->insert( { key => $_ } ) for 1 .. 2;
    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->find( [] )->replace_one( [ key => 3 ] );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on replace" ) or diag explain $err;
    is( $coll->count( { key => 3 } ), 1, "only one doc replaced" );
};

note("Tie::IxHash");
subtest "insert (Tie::IxHash)" => sub {
    $coll->drop;
    my $bulk = $coll->initialize_ordered_bulk_op;
    is( $coll->count, 0, "no docs in collection" );
    $bulk->insert( Tie::IxHash->new( _id => 1 ) );
    $bulk->insert( Tie::IxHash->new() );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on insert" ) or diag explain $err;
    is( $coll->count, 2, "doc count" );
};

subtest "update (Tie::IxHash)" => sub {
    $coll->drop;
    $coll->insert( { _id => 1 } );
    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->find( Tie::IxHash->new() )
      ->update( Tie::IxHash->new( '$set' => { x => 2 } ) );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on update" ) or diag explain $err;
    is( $coll->find_one( {} )->{x}, 2, "document updated" );
};

subtest "update_one (Tie::IxHash)" => sub {
    $coll->drop;
    $coll->insert( { _id => $_ } ) for 1 .. 2;
    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->find( Tie::IxHash->new() )
      ->update_one( Tie::IxHash->new( '$set' => { x => 2 } ) );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on update" ) or diag explain $err;
    is( $coll->count( { x => 2 } ), 1, "only one doc updated" );
};

subtest "replace_one (Tie::IxHash)" => sub {
    $coll->drop;
    $coll->insert( { key => $_ } ) for 1 .. 2;
    my $bulk = $coll->initialize_ordered_bulk_op;
    $bulk->find( Tie::IxHash->new() )->replace_one( Tie::IxHash->new( key => 3 ) );
    my ( $result, $err );
    $err = exception { $result = $bulk->execute };
    is( $err, undef, "no error on replace" ) or diag explain $err;
    is( $coll->count( { key => 3 } ), 1, "only one doc replaced" );
};

# not in QA-477
note("W = 0 IGNORES ERRORS");
for my $method (qw/initialize_ordered_bulk_op initialize_unordered_bulk_op/) {
    subtest "$method: w = 0" => sub {
        $coll->drop;
        my $bulk = $coll->$method;

        $bulk->insert( { _id => 1 } );
        $bulk->insert( { _id => 2, big => "a" x ( 16 * 1024 * 1024 ) } );
        $bulk->insert( { _id => 3, '$bad' => 1 } );
        $bulk->insert( { _id => 4 } );
        my ( $result, $err );
        $err = exception { $result = $bulk->execute( { w => 0 } ) };
        is( $err, undef, "execute with w = 0 doesn't throw error" )
          or diag explain $err;

        my $expect = $method eq 'initialize_ordered_bulk_op' ? 1 : 2;
        is( $coll->count, $expect, "document count ($expect)" );
    };
}

# XXX QA-477 tests not covered herein:
# MIXED OPERATIONS, AUTH
# FAILOVER WITH MIXED VERSIONS

done_testing;
