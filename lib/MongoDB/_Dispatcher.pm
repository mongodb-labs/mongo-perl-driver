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
package MongoDB::_Dispatcher;

# Encapsulate op dispatching; breaking this out from client
# allows avoiding circular references with the session pool class.

use version;
our $VERSION = 'v2.2.2';

use Moo;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    Boolish
);
use Carp;
use Types::Standard qw(
    InstanceOf
);
use Safe::Isa;

use namespace::clean;

has topology => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::_Topology'],
);

has retry_writes => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has retry_reads => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

# Reset session state if we're outside an active transaction, otherwise set
# that this transaction actually has operations
sub _maybe_update_session_state {
    my ( $self, $op ) = @_;
    if ( defined $op->session && ! $op->session->_active_transaction ) {
        $op->session->_set__transaction_state( TXN_NONE );
    } elsif ( defined $op->session ) {
        $op->session->_set__has_transaction_operations( 1 );
    }
}

# op dispatcher written in highly optimized style
sub send_direct_op {
    my ( $self, $op, $address ) = @_;
    my ( $link, $result );

    $self->_maybe_update_session_state( $op );

    ( $link = $self->{topology}->get_specific_link( $address, $op ) ), (
        eval { ($result) = $op->execute($link); 1 } or do {
            my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            if ( $err->$_isa("MongoDB::ConnectionError") || $err->$_isa("MongoDB::NetworkTimeout") ) {
                $self->{topology}->mark_server_unknown( $link->server, $err );
            }
            elsif ( $err->$_isa("MongoDB::NotMasterError") ) {
                $self->{topology}->mark_server_unknown( $link->server, $err );
                $self->{topology}->mark_stale;
            }
            # regardless of cleanup, rethrow the error
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
          }
      ),
      return $result;
}

sub _retrieve_link_for {
    my ( $self, $op, $rw ) = @_;
    my $topology = $self->{'topology'};
    my $link;
    if ( $op->session
        && $op->session->_address # no point trying if theres no address....
        && $op->session->_active_transaction # this is true during a transaction and on every commit
        && $topology->_supports_mongos_pinning_transactions )
    {
        $link = $topology->get_specific_link( $op->session->_address, $op );
    }
    elsif ( $rw eq 'w' ) {
        $link = $topology->get_writable_link( $op );
    } else {
        $link = $topology->get_readable_link( $op );
    }
    return $link;
}

# op dispatcher written in highly optimized style
sub send_write_op {
    my ( $self, $op ) = @_;
    my ( $link, $result );

    $self->_maybe_update_session_state( $op );

    ( $link = $self->_retrieve_link_for( $op, 'w' ) ), (
        eval { ($result) = $self->_try_op_for_link( $link, $op ); 1 } or do {
            my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        }
      ),
      return $result;
}

# Sometimes, seeing an op dispatched as "send_write_op" is confusing when
# really, we're just insisting that it be sent only to a primary or
# directly connected server.
BEGIN {
    no warnings 'once';
    *send_primary_op = \&send_write_op;
}

sub send_retryable_write_op {
    my ( $self, $op, $force ) = @_;
    my ( $link, $result ) = ( $self->_retrieve_link_for( $op, 'w' ) );

    $self->_maybe_update_session_state( $op );

    # Need to force to do a retryable write on a Transaction Commit or Abort.
    # $force is an override for retry_writes, but theres no point trying that
    # if the link doesnt support it anyway.
    # This triggers on the following:
    # * $force is not set to 'force'
    #   (specifically for retrying writes in ending transaction operations)
    # * retry writes is not enabled or the link doesnt support retryWrites
    # * if an active transaction is starting or in progress
    unless ( $link->supports_retryWrites
        && ( $self->retry_writes || ( defined $force && $force eq 'force' ) )
        && ( defined $op->session
          && ! $op->session->_in_transaction_state( TXN_STARTING, TXN_IN_PROGRESS )
        )
    ) {
        eval { ($result) = $self->_try_op_for_link( $link, $op ); 1 } or do {
            my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        };
        return $result;
    }

    # If we get this far and there is no session, then somethings gone really
    # wrong, so probably not worth worrying about.

    # increment transaction id before write, but otherwise is the same for both
    # attempts. If not in a transaction, is a no-op
    $op->session->_increment_transaction_id;
    $op->retryable_write( 1 );

    # attempt the op the first time
    eval { ($result) = $self->_try_op_for_link( $link, $op ); 1 } or do {
        my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";

        if ( $err->$_call_if_can('_is_storage_engine_not_retryable') ) {
            # Break encapsulation to rewrite the message, then rethrow.
            $err->{message} = "This MongoDB deployment does not support retryable writes. Please add retryWrites=false to your connection string.";
            die $err;
        }

        # If the error is not retryable, then drop out
        unless ( $err->$_call_if_can('_is_retryable') ) {
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        }

        # Must check if error is retryable before getting the link, in case we
        # get a 'no writable servers' error. In the case of a mongos retry,
        # this will end up as the same server by design.
        my $retry_link = $self->_retrieve_link_for( $op, 'w' );

        # Rare chance that the new link is not retryable
        unless ( $retry_link->supports_retryWrites ) {
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        }

        # Second attempt
        eval { ($result) = $self->_try_op_for_link( $retry_link, $op ); 1 } or do {
            my $retry_err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            WITH_ASSERTS ? ( confess $retry_err ) : ( die $retry_err );
        };
    };
    # just in case this gets reused for some reason
    $op->retryable_write( 0 );
    return $result;
}

sub _is_primary_stepdown {
    my ($self, $err, $link) = @_;
    my $err_info = $err->{result}->{output};
    my $err_code_name = '';
    $err_code_name = $err_info->{'codeName'} if defined $err_info->{'codeName'};
    my @other_errors = qw(ShutdownInProgress InterruptedAtShutdown);
    my $not_master = (
        $err->$_isa('MongoDB::NotMasterError')
            || ( $err_info && $err_code_name eq 'NotMaster' )
    ) && $link->max_wire_version < 8;
    return (
        $err_info && grep { $err_code_name eq $_ } @other_errors
    ) || $not_master;
}

# op dispatcher written in highly optimized style
sub _try_op_for_link {
    my ( $self, $link, $op ) = @_;
    my $result;
    (
        eval { ($result) = $op->execute($link, $self->{topology}->type); 1 } or do {
            my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            if ( $err->$_isa("MongoDB::ConnectionError") || $err->$_isa("MongoDB::NetworkTimeout") ) {
                $self->{topology}->mark_server_unknown( $link->server, $err );
            }
            elsif ( $self->_is_primary_stepdown($err, $link) ) {
                $self->{topology}->mark_server_unknown( $link->server, $err );
                $self->{topology}->mark_stale;
            }
            # normal die here instead of assert, which is used later
            die $err;
        }
    ),
    return $result;
}

sub send_retryable_read_op {
    my ( $self, $op ) = @_;
    my $result;

    # Get transaction read preference if in a transaction.
    if ( defined $op->session && $op->session->_active_transaction ) {
        # Transactions may only read from primary in MongoDB 4.0, so get and
        # check the read preference from the transaction settings as per
        # transaction spec - see MongoDB::_TransactionOptions
        $op->read_preference( $op->session->_get_transaction_read_preference );
    }

    my $link = $self->_retrieve_link_for( $op, 'r' );

    $self->_maybe_update_session_state( $op );

    if ( ! $link->supports_retryReads
        || ! $self->retry_reads
        || ( defined $op->session && $op->session->_in_transaction_state( TXN_STARTING, TXN_IN_PROGRESS ))
    ) {
        eval { ($result) = $self->_try_op_for_link( $link, $op ); 1 } or do {
            my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        };
        return $result;
    }

    $op->session->_increment_transaction_id if $op->session;

    $op->retryable_read( 1 );
    # attempt the op the first time
    eval { ($result) = $self->_try_op_for_link( $link, $op ); 1 } or do {
        my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";

        # If the error is not retryable, then drop out
        unless ( $err->$_call_if_can('_is_retryable') ) {
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        }

        my $retry_link = $self->_retrieve_link_for( $op, 'r' );

        # Rare chance that the new link is not retryable
        unless ( $retry_link->supports_retryReads ) {
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
        }

        # Second attempt
        eval { ($result) = $self->_try_op_for_link( $retry_link, $op ); 1 } or do {
            my $retry_err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
                WITH_ASSERTS ? ( confess $retry_err ) : ( die $retry_err );
        };
    };
    # just in case this gets reused for some reason
    $op->retryable_read( 0 );

    return $result;
}

# op dispatcher written in highly optimized style
sub send_read_op {
    my ( $self, $op ) = @_;
    my ( $link, $type, $result );

    # Get transaction read preference if in a transaction.
    if ( defined $op->session && $op->session->_active_transaction ) {
        # Transactions may only read from primary in MongoDB 4.0, so get and
        # check the read preference from the transaction settings as per
        # transaction spec - see MongoDB::_TransactionOptions
        $op->read_preference( $op->session->_get_transaction_read_preference );
    }

    $self->_maybe_update_session_state( $op );

    ( $link = $self->_retrieve_link_for( $op, 'r' ) ),
      ( $type = $self->{topology}->type ), (
        eval { ($result) = $op->execute( $link, $type ); 1 } or do {
            my $err = length($@) ? $@ : "caught error, but it was lost in eval unwind";
            if ( $err->$_isa("MongoDB::ConnectionError") || $err->$_isa("MongoDB::NetworkTimeout") ) {
                $self->{topology}->mark_server_unknown( $link->server, $err );
            }
            elsif ( $err->$_isa("MongoDB::NotMasterError") ) {
                $self->{topology}->mark_server_unknown( $link->server, $err );
                $self->{topology}->mark_stale;
            }
            # regardless of cleanup, rethrow the error
            WITH_ASSERTS ? ( confess $err ) : ( die $err );
          }
      ),
      return $result;
}

1;
