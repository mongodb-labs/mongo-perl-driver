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

use strict;
use warnings;
package MongoDB::ClientSession;

# ABSTRACT: MongoDB session and transaction management

use version;
our $VERSION = 'v2.1.0';

use MongoDB::Error;

use Moo;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    Document
    BSONTimestamp
    TransactionState
    Boolish
);
use Types::Standard qw(
    Maybe
    HashRef
    InstanceOf
);
use MongoDB::_TransactionOptions;
use namespace::clean -except => 'meta';

=attr client

The client this session was created using.  Sessions may only be used
with the client that created them.

=cut

has client => (
    is => 'ro',
    isa => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

=attr cluster_time

Stores the last received C<$clusterTime> for the client session. This is an
opaque value, to set it use the L<advance_cluster_time> function.

=cut

has cluster_time => (
    is => 'rwp',
    isa => Maybe[Document],
    init_arg => undef,
    default => undef,
);

=attr options

Options provided for this particular session. Available options include:

=for :list
* C<causalConsistency> - If true, will enable causalConsistency for
  this session. For more information, see L<MongoDB documentation on Causal
  Consistency|https://docs.mongodb.com/manual/core/read-isolation-consistency-recency/#causal-consistency>.
  Note that causalConsistency does not apply for unacknowledged writes.
  Defaults to true.
* C<defaultTransactionOptions> - Options to use by default for transactions
  created with this session. If when creating a transaction, none or only some of
  the transaction options are defined, these options will be used as a fallback.
  Defaults to inheriting from the parent client. See L</start_transaction> for
  available options.

=cut

has options => (
    is => 'ro',
    isa => HashRef,
    required => 1,
    # Shallow copy to prevent action at a distance.
    # Upgrade to use Storable::dclone if a more complex option is required
    coerce => sub {
        # Will cause the isa requirement to fire
        return unless defined( $_[0] ) && ref( $_[0] ) eq 'HASH';
        $_[0] = {
            causalConsistency => defined $_[0]->{causalConsistency}
                ? $_[0]->{causalConsistency}
                : 1,
            defaultTransactionOptions => {
                %{ $_[0]->{defaultTransactionOptions} || {} }
            },
        };
    },
);

has _server_session => (
    is => 'ro',
    isa => InstanceOf['MongoDB::_ServerSession'],
    init_arg => 'server_session',
    required => 1,
    clearer => '__clear_server_session',
);

has _current_transaction_options => (
    is => 'rwp',
    isa => InstanceOf[ 'MongoDB::_TransactionOptions' ],
    handles  => {
        _get_transaction_write_concern   => 'write_concern',
        _get_transaction_read_concern    => 'read_concern',
        _get_transaction_read_preference => 'read_preference',
    },
);

has _transaction_state => (
    is => 'rwp',
    isa => TransactionState,
    default => 'none',
);

# Flag used to say we are still in a transaction
has _active_transaction => (
    is => 'rwp',
    isa => Boolish,
    default => 0,
);

# Flag used to say whether any operations have been performed on the
# transaction
has _has_transaction_operations => (
    is => 'rwp',
    isa => Boolish,
    default => 0,
);

=attr operation_time

The last operation time. This is updated when an operation is performed during
this session, or when L</advance_operation_time> is called. Used for causal
consistency.

=cut

has operation_time => (
    is => 'rwp',
    isa => Maybe[BSONTimestamp],
    init_arg => undef,
    default => undef,
);

=method session_id

The session id for this particular session.  This should be considered
an opaque value.  If C<end_session> has been called, this returns C<undef>.

=cut

sub session_id {
    my ($self) = @_;
    return defined $self->_server_session ? $self->_server_session->session_id : undef;
}

=method get_latest_cluster_time

    my $cluster_time = $session->get_latest_cluster_time;

Returns the latest cluster time, when compared with this session's recorded
cluster time and the main client cluster time. If neither is defined, returns
undef.

=cut

sub get_latest_cluster_time {
    my ( $self ) = @_;

    # default to the client cluster time - may still be undef
    if ( ! defined $self->cluster_time ) {
        return $self->client->_cluster_time;
    }

    if ( defined $self->client->_cluster_time ) {
        # Both must be defined here so can just compare
        if ( $self->cluster_time->{'clusterTime'}
          > $self->client->_cluster_time->{'clusterTime'} ) {
            return $self->cluster_time;
        } else {
            return $self->client->_cluster_time;
        }
    }

    # Could happen that this cluster_time is updated manually before the client
    return $self->cluster_time;
}


=method advance_cluster_time

    $session->advance_cluster_time( $cluster_time );

Update the C<$clusterTime> for this session. Stores the value in
L</cluster_time>. If the cluster time provided is more recent than the sessions
current cluster time, then the session will be updated to this provided value.

Setting the C<$clusterTime> with a manually crafted value may cause a server
error. It is recommended to only use C<$clusterTime> values retrieved from
database calls.

=cut

sub advance_cluster_time {
    my ( $self, $cluster_time ) = @_;

    return unless $cluster_time && exists $cluster_time->{clusterTime}
        && ref($cluster_time->{clusterTime}) eq 'BSON::Timestamp';

    # Only update the cluster time if it is more recent than the current entry
    if ( ! defined $self->cluster_time ) {
        $self->_set_cluster_time( $cluster_time );
    } else {
        if ( $cluster_time->{'clusterTime'}
          > $self->cluster_time->{'clusterTime'} ) {
            $self->_set_cluster_time( $cluster_time );
        }
    }
    return;
}

=method advance_operation_time

    $session->advance_operation_time( $operation_time );

Update the L</operation_time> for this session. If the value provided is more
recent than the sessions current operation time, then the session will be
updated to this provided value.

Setting C<operation_time> with a manually crafted value may cause a server
error. It is recommended to only use an C<operation_time> retrieved from
another session or directly from a database call.

=cut

sub advance_operation_time {
    my ( $self, $operation_time ) = @_;

    # Just dont update operation_time if they've denied this, as it'l stop
    # everywhere else that updates based on this value from the session
    return unless $self->options->{causalConsistency};

    if ( !defined( $self->operation_time )
      || ( $operation_time > $self->operation_time ) ) {
        $self->_set_operation_time( $operation_time );
    }
    return;
}

# Returns 1 if the session is in one of the specified transaction states.
# Returns a false value if not in any of the states defined as an argument.
sub _in_transaction_state {
    my ( $self, @states ) = @_;
    return 1 if scalar ( grep { $_ eq $self->_transaction_state } @states );
    return;
}

=method start_transaction

    $session->start_transaction;
    $session->start_transaction( $options );

Start a transaction in this session.  If a transaction is already in
progress or if the driver can detect that the client is connected to a
topology that does not support transactions, this method will throw an
error.

A hash reference of options may be provided. Valid keys include:

=for :list
* C<readConcern> - The read concern to use for the first command in this
  transaction. If not defined here or in the C<defaultTransactionOptions> in
  L</options>, will inherit from the parent client.
* C<writeConcern> - The write concern to use for committing or aborting this
  transaction. As per C<readConcern>, if not defined here then the value defined
  in C<defaultTransactionOptions> will be used, or the parent client if not
  defined.
* C<readPreference> - The read preference to use for all read operations in
  this transaction. If not defined, then will inherit from
  C<defaultTransactionOptions> or from the parent client. This value will
  override all other read preferences set in any subsequent commands inside this
  transaction.

=cut

sub start_transaction {
    my ( $self, $opts ) = @_;

    MongoDB::UsageError->throw("Transaction already in progress")
        if $self->_in_transaction_state( TXN_STARTING, TXN_IN_PROGRESS );

    MongoDB::ConfigurationError->throw("Transactions are unsupported on this deployment")
        unless $self->client->_topology->_supports_transactions;

    $opts ||= {};
    my $trans_opts = MongoDB::_TransactionOptions->new(
        client => $self->client,
        options => $opts,
        default_options => $self->options->{defaultTransactionOptions},
    );

    $self->_set__current_transaction_options( $trans_opts );

    $self->_set__transaction_state( TXN_STARTING );

    $self->_increment_transaction_id;

    $self->_set__active_transaction( 1 );
    $self->_set__has_transaction_operations( 0 );

    return;
}

sub _increment_transaction_id {
    my $self = shift;
    return if $self->_active_transaction;

    $self->_server_session->transaction_id->binc();
}

=method commit_transaction

    $session->commit_transaction;

Commit the current transaction. This will use the writeConcern set on this
transaction.

If called when no transaction is in progress, then this method will throw
an error.

If the commit operation encounters an error, an error is thrown.  If the
error is a transient commit error, the error object will have a label
containing "UnknownTransactionCommitResult" as an element and the commit
operation can be retried.  This can be checked via the C<has_error_label>:

    LOOP: {
        eval {
            $session->commit_transaction;
        };
        if ( my $error = $@ ) {
            if ( $error->has_error_label("UnknownTransactionCommitResult") ) {
                redo LOOP;
            }
            else {
                die $error;
            }
        }
    }

=cut

sub commit_transaction {
    my $self = shift;

    MongoDB::UsageError->throw("No transaction started")
        if $self->_in_transaction_state( TXN_NONE );

    # Error message tweaked to use our function names
    MongoDB::UsageError->throw("Cannot call commit_transaction after calling abort_transaction")
        if $self->_in_transaction_state( TXN_ABORTED );

    # Commit can be called multiple times - even if the transaction completes
    # correctly. Setting this here makes sure we dont increment transaction id
    # until after another command has been called using this session
    $self->_set__active_transaction( 1 );

    eval {
        $self->_send_end_transaction_command( TXN_COMMITTED, [ commitTransaction => 1 ] );
    };
    if ( my $err = $@ ) {
        # catch and re-throw after retryable errors
        my $err_code_name;
        my $err_code;
        if ( $err->can('result') ) {
            if ( $err->result->can('output') ) {
                $err_code_name = $err->result->output->{codeName};
                $err_code = $err->result->output->{code};
                $err_code_name ||= $err->result->output->{writeConcernError}
                    ? $err->result->output->{writeConcernError}->{codeName}
                    : ''; # Empty string just in case
                $err_code ||= $err->result->output->{writeConcernError}
                    ? $err->result->output->{writeConcernError}->{code}
                    : 0; # just in case
            }
        }
        # If its a write concern error, retrying a commit would still error
        unless (
            ( defined( $err_code_name ) && grep { $_ eq $err_code_name } qw/
                CannotSatisfyWriteConcern
                UnsatisfiableWriteConcern
                UnknownReplWriteConcern
                NoSuchTransaction
            / )
            # Spec tests include code numbers only with no codeName
            || ( defined ( $err_code ) && grep { $_ == $err_code }
                100, # UnsatisfiableWriteConcern/CannotSatisfyWriteConcern
                79,  # UnknownReplWriteConcern
                251, # NoSuchTransaction
            )
        ) {
            push @{ $err->error_labels }, 'UnknownTransactionCommitResult';
        }
        die $err;
    }

    return;
}

=method abort_transaction

    $session->abort_transaction;

Aborts the current transaction.  If no transaction is in progress, then this
method will throw an error.  Otherwise, this method will suppress all other
errors (including network and database errors).

=cut

sub abort_transaction {
    my $self = shift;

    MongoDB::UsageError->throw("No transaction started")
        if $self->_in_transaction_state( TXN_NONE );

    # Error message tweaked to use our function names
    MongoDB::UsageError->throw("Cannot call abort_transaction after calling commit_transaction")
        if $self->_in_transaction_state( TXN_COMMITTED );

    # Error message tweaked to use our function names
    MongoDB::UsageError->throw("Cannot call abort_transaction twice")
        if $self->_in_transaction_state( TXN_ABORTED );

    eval {
        $self->_send_end_transaction_command( TXN_ABORTED, [ abortTransaction => 1 ] );
    };
    # Ignore all errors thrown by abortTransaction

    return;
}

sub _send_end_transaction_command {
    my ( $self, $end_state, $command ) = @_;

    $self->_set__transaction_state( $end_state );

    # Only need to send commit command if the transaction actually sent anything
    if ( $self->_has_transaction_operations ) {
        my $op = MongoDB::Op::_Command->_new(
            db_name             => 'admin',
            query               => $command,
            query_flags         => {},
            bson_codec          => $self->client->bson_codec,
            session             => $self,
            monitoring_callback => $self->client->monitoring_callback,
        );

        my $result = $self->client->send_retryable_write_op( $op, 'force' );
        $result->assert_no_write_concern_error;
    }

    # If the commit/abort succeeded, we are no longer in an active transaction
    $self->_set__active_transaction( 0 );
}

# For applying connection errors etc
sub _maybe_apply_error_labels {
    my ( $self, $err ) = @_;

    if ( $self->_in_transaction_state( TXN_STARTING, TXN_IN_PROGRESS ) ) {
        push @{ $err->error_labels }, 'TransientTransactionError';
    }
    return;
}

=method end_session

    $session->end_session;

Close this particular session and release the session ID for reuse or
recycling.  If a transaction is in progress, it will be aborted.  Has no
effect after calling for the first time.

This will be called automatically by the object destructor.

=cut

sub end_session {
    my ( $self ) = @_;

    if ( $self->_in_transaction_state ( TXN_IN_PROGRESS ) ) {
        # Ignore all errors
        eval { $self->abort_transaction };
    }
    if ( defined $self->_server_session ) {
        $self->client->_server_session_pool->retire_server_session( $self->_server_session );
        $self->__clear_server_session;
    }
}

sub DEMOLISH {
    my ( $self, $in_global_destruction ) = @_;
    # Implicit end of session in scope
    $self->end_session;
}

1;

__END__

=pod

=head1 SYNOPSIS

    my $session = $client->start_session( $options );

    # use session in operations
    my $result = $collection->find( { id => 1 }, { session => $session } );

    # use sessions for transactions
    $session->start_transaction;
    ...
    if ( $ok ) {
        $session->commit_transaction;
    }
    else {
        $session->abort_transaction;
    }

=head1 DESCRIPTION

This class encapsulates an active session for use with the current client.
Sessions support is new with MongoDB 3.6, and can be used in replica set and
sharded MongoDB clusters.

=head2 Explicit and Implicit Sessions

If you specifically apply a session to an operation, then the operation will be
performed with that session id. If you do not provide a session for an
operation, and the server supports sessions, then an implicit session will be
created and used for this operation.

The only exception to this is for unacknowledged writes - the driver will not
provide an implicit session for this, and if you provide a session then the
driver will raise an error.

=head2 Cursors

During cursors, if a session is not provided then an implicit session will be
created which is then used for the lifetime of the cursor. If you provide a
session, then note that ending the session and then continuing to use the
cursor will raise an error.

=head2 Thread Safety

Sessions are NOT thread safe, and should only be used by one thread at a time.
Using a session across multiple threads is unsupported and unexpected issues
and errors may occur. Note that the driver does not check for multi-threaded
use.

=head2 Transactions

A session may be associated with at most one open transaction (on MongoDB
4.0+).  For detailed instructions on how to use transactions with drivers,
see the MongoDB manual page:
L<Transactions|https://docs.mongodb.com/master/core/transactions>.

=cut
