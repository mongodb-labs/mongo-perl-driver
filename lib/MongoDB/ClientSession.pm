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

# ABSTRACT: MongoDB session management

use version;
our $VERSION = 'v1.999.1';

use MongoDB::Error;

use Moo;
use MongoDB::ReadConcern;
use MongoDB::_Types qw(
    Document
    BSONTimestamp
    TransactionState
);
use Types::Standard qw(
    Maybe
    HashRef
    InstanceOf
);
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
      $_[0] = {
        causalConsistency => 1,
        %{ $_[0] },
        # applied after to not override the clone with the original
        defaultTransactionOptions => {
          defined( $_[0] )
            && ref( $_[0] ) eq 'HASH'
            && defined( $_[0]->{defaultTransactionOptions} )
              ? ( %{ $_[0]->{defaultTransactionOptions} } )
              : (),
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

has _current_transaction_settings => (
    is => 'rwp',
    isa => HashRef,
    init_arg => undef,
    clearer => '_clear_current_transaction_settings',
);

has _transaction_state => (
    is => 'rwp',
    isa => TransactionState,
    default => 'none',
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
error. It is reccomended to only use C<$clusterTime> values retrieved from
database calls.

=cut

sub advance_cluster_time {
    my ( $self, $cluster_time ) = @_;

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
error. It is recommended to only use an C<operation_time> retreived from
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

Start a transaction in this session. Takes a hashref of options which can contain the following options:

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

    MongoDB::TransactionError->throw("Transaction already in progress")
        if $self->_in_transaction_state( 'starting', 'in_progress' );

    MongoDB::ConfigurationError->throw("Transactions are unsupported on this deployment")
        unless $self->client->_topology->_supports_transactions;

    $opts ||= {};
    $opts = { %{ $self->options->{defaultTransactionOptions} }, %$opts };

    $self->_set__current_transaction_settings( $opts );

    $self->_set__transaction_state('starting');

    $self->_increment_transaction_id;

    return;
}

sub _increment_transaction_id {
    my $self = shift;
    return if $self->_in_transaction_state( qw/ in_progress committed aborted / );

    $self->_server_session->transaction_id->binc();
}

=method commit_transaction

Commit the current transaction. This will use the writeConcern set on this transaction.

=cut

sub commit_transaction {
    my $self = shift;

    MongoDB::TransactionError->throw("No transaction started")
        if $self->_transaction_state eq 'none';

    # Error message tweaked to use our function names
    MongoDB::TransactionError->throw("Cannot call commit_transaction after calling abort_transaction")
        if $self->_transaction_state eq 'aborted';

    $self->_send_end_transaction_command( 'committed', [ commitTransaction => 1 ] );

    return;
}

=method abort_transaction

Abort the current transaction. This will use the writeConcern set on this transaction.

=cut

sub abort_transaction {
    my $self = shift;

    MongoDB::TransactionError->throw("No transaction started")
        if $self->_in_transaction_state( 'none' );

    # Error message tweaked to use our function names
    MongoDB::TransactionError->throw("Cannot call abort_transaction after calling commit_transaction")
        if $self->_in_transaction_state( 'committed' );

    # Error message tweaked to use our function names 
    MongoDB::TransactionError->throw("Cannot call abort_transaction twice")
        if $self->_in_transaction_state( 'aborted' );

    $self->_send_end_transaction_command( 'aborted', [ abortTransaction => 1 ] );

    return;
}

sub _send_end_transaction_command {
    my ( $self, $end_state, $command ) = @_;

    # Only need to send commit command if the transaction actually sent anything
    if ( ! $self->_in_transaction_state( qw/ starting / ) ) {

        # Must set state before running the op as otherwise it wont be retried
        $self->_set__transaction_state( $end_state );

        my $op = MongoDB::Op::_Command->_new(
            db_name             => 'admin',
            query               => $command,
            query_flags         => {},
            bson_codec          => $self->client->bson_codec,
            session             => $self,
            monitoring_callback => $self->client->monitoring_callback,
        );

        $self->client->send_retryable_write_op( $op, 'force' );
    }

    $self->_set__transaction_state( $end_state );
}

sub _get_transaction_read_concern {
    my $self = shift;
    # readConcern is merged during start_transaction
    if ( defined $self->_current_transaction_settings->{readConcern} ) {
        return MongoDB::ReadConcern->new( $self->_current_transaction_settings->{readConcern} );
    }

    # Default to the clients read concern
    return $self->client->read_concern;
}

sub _get_transaction_write_concern {
    my $self = shift;
    # writeConcern is merged during start_transaction
    if ( defined $self->_current_transaction_settings->{writeConcern} ) {
        return MongoDB::WriteConcern->new( $self->_current_transaction_settings->{writeConcern} );
    }

    # Default to client write_concern, however unlikely to actually be used
    return $self->client->write_concern;
}

# TODO TBSliver REMOVE ME ON RELEASE
sub _debug {
    my $self = shift;
    return {
        state           => $self->_transaction_state,
        client          => defined $self->client ? 'defined' : '',
        session         => defined $self->_server_session ? 'defined' : '',
        session_id      => $self->session_id,
        transaction_id  => defined $self->_server_session ? $self->_server_session->transaction_id : '',
        cluster_time    => $self->cluster_time,
        options         => $self->options,
        transaction_settings => $self->_current_transaction_settings,
        operation_time  => $self->operation_time,
    };
}

=method end_session

    $session->end_session;

Close this particular session and release the session ID for reuse or
recycling.  Has no effect after calling for the first time.

=cut

sub end_session {
    my ( $self ) = @_;

    if ( $self->_transaction_state eq 'in_progress' ) {
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

=cut
