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

# MongoDB documentation examples in Perl.

# NOTE: Developers: Do not change these examples without approval of the
# MongoDB documentation team as they are extracted to populate examples
# on the MongoDB docs website.

use strict;
use warnings;
use Test::More 0.96;

use MongoDB;
use Tie::IxHash;
use boolean;

use lib "t/lib";
use MongoDBTest qw(
  build_client
  skip_unless_mongod
  skip_unless_transactions
);

skip_unless_mongod();
skip_unless_transactions();

my $client = build_client();

# Fixtures

$client->db("hr")->drop;
$client->db("reporting")->drop;
my $employees = $client->ns("hr.employees");
my $events    = $client->ns("reporting.events");
$employees->insert_one( { employee => 3, status => "Active" } );
$events->insert_one(
    { employee => 3, status => { new => "Active", old => undef } } );

#<<< No perltidy

package dummy::package::to::avoid::redefined::sub;

# Start Transactions Intro Example 1

sub updateEmployeeInfo {
    my ($session)           = @_;
    my $employeesCollection = $session->client->ns("hr.employees");
    my $eventsCollection    = $session->client->ns("reporting.events");

    $session->start_transaction(
        {
            readConcern  => { level => "snapshot" },
            writeConcern => { w     => "majority" },
        }
    );

    eval {
        $employeesCollection->update_one(
            { session => $session},
            { employee => 3 }, { '$set' => { status => "Inactive" } },
        );
        $eventsCollection->insert_one(
            { session => $session},
            { employee => 3, status => { new => "Inactive", old => "Active" } },
        );
    };
    if ( my $error = $@ ) {
        print("Caught exception during transaction, aborting->\n");
        $session->abort_transaction();
        die $error;
    }

    LOOP: {
        eval {
            $session->commit_transaction(); # Uses write concern set at transaction start.
            print("Transaction committed->\n");
        };
        if ( my $error = $@ ) {
            # Can retry commit
            if ( $error->has_error_label("UnknownTransactionCommitResult") ) {
                print("UnknownTransactionCommitResult, retrying commit operation ->..\n");
                redo LOOP;
            }
            else {
                print("Error during commit ->..\n");
                die $error;
            }
        }
    }

    return;
}
# End Transactions Intro Example 1


# Start Transactions Retry Example 1
sub runTransactionWithRetry {
    my ( $txnFunc, $session ) = @_;

    LOOP: {
        eval {
            $txnFunc->($session); # performs transaction
        };
        if ( my $error = $@ ) {
            print("Transaction aborted-> Caught exception during transaction.\n");
            # If transient error, retry the whole transaction
            if ( $error->has_error_label("TransientTransactionError") ) {
                print("TransientTransactionError, retrying transaction ->..\n");
                redo LOOP;
            }
            else {
                die $error;
            }
        }
    }

    return;
}
# End Transactions Retry Example 1

# Start Transactions Retry Example 2
sub commitWithRetry {
    my ($session) = @_;

    LOOP: {
        eval {
            $session->commit_transaction(); # Uses write concern set at transaction start.
            print("Transaction committed->\n");
        };
        if ( my $error = $@ ) {
            # Can retry commit
            if ( $error->has_error_label("UnknownTransactionCommitResult") ) {
                print("UnknownTransactionCommitResult, retrying commit operation ->..\n");
                redo LOOP;
            }
            else {
                print("Error during commit ->..\n");
                die $error;
            }
        }
    }

    return;
}
# End Transactions Retry Example 2

package main;

# Start Transactions Retry Example 3
sub runTransactionWithRetry {
    my ( $txnFunc, $session ) = @_;

    LOOP: {
        eval {
            $txnFunc->($session); # performs transaction
        };
        if ( my $error = $@ ) {
            print("Transaction aborted-> Caught exception during transaction.\n");
            # If transient error, retry the whole transaction
            if ( $error->has_error_label("TransientTransactionError") ) {
                print("TransientTransactionError, retrying transaction ->..\n");
                redo LOOP;
            }
            else {
                die $error;
            }
        }
    }

    return;
}

sub commitWithRetry {
    my ($session) = @_;

    LOOP: {
        eval {
            $session->commit_transaction(); # Uses write concern set at transaction start.
            print("Transaction committed->\n");
        };
        if ( my $error = $@ ) {
            # Can retry commit
            if ( $error->has_error_label("UnknownTransactionCommitResult") ) {
                print("UnknownTransactionCommitResult, retrying commit operation ->..\n");
                redo LOOP;
            }
            else {
                print("Error during commit ->..\n");
                die $error;
            }
        }
    }

    return;
}

# Updates two collections in a transactions

sub updateEmployeeInfo {
    my ($session) = @_;
    my $employeesCollection = $session->client->ns("hr.employees");
    my $eventsCollection    = $session->client->ns("reporting.events");

    $session->start_transaction(
        {
            readConcern  => { level => "snapshot" },
            writeConcern => { w     => "majority" },
        }
    );

    eval {
        $employeesCollection->update_one(
            { session => $session},
            { employee => 3 }, { '$set' => { status => "Inactive" } },
        );
        $eventsCollection->insert_one(
            { session => $session},
            { employee => 3, status => { new => "Inactive", old => "Active" } },
        );
    };
    if ( my $error = $@ ) {
        print("Caught exception during transaction, aborting->\n");
        $session->abort_transaction();
        die $error;
    }

    commitWithRetry($session);
}

# Start a session
my $session = $client->start_session();

eval {
    runTransactionWithRetry(\&updateEmployeeInfo, $session);
};
if ( my $error = $@ ) {
    # Do something with error
}

$session->end_session();

# End Transactions Retry Example 3

#>>> no perltidy

# Test transaction ran
my $employee = $employees->find_one({ employee => 3 });
ok( $employee, "Found employee" );
is( $employee->{status}, "Inactive", "status updated" );

done_testing;
