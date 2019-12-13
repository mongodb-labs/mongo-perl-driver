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
package MongoDB::Op::_EndTxn;

# Encapsulate code path for end transaction commands

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::_Types qw(
    Document
    to_IxHash
);
use MongoDB::_Constants qw( TXN_WTIMEOUT_RETRY_DEFAULT );
use Types::Standard qw(
    HashRef
    Maybe
    Int
);

use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseOp
  MongoDB::Role::_SessionSupport
  MongoDB::Role::_CommandMonitoring
);

has query => (
    is       => 'ro',
    required => 1,
    writer   => '_set_query',
    isa      => Document,
);

sub _get_query_maybe_with_write_concern {
    my ( $self, $topology ) = @_;

    my $query = to_IxHash( $self->query );

    if ( $self->session->_has_attempted_end_transaction ) {
        my $wc_existing = $self->session->_get_transaction_write_concern;
        my $wc = $wc_existing->as_args->[1];

        $query->Push( writeConcern => {
            # allows for an override if set
            wtimeout => TXN_WTIMEOUT_RETRY_DEFAULT,
            ( $wc ? %$wc : () ),
            # must be a majority on retrying
            w => 'majority',
        });
    }

    # If we've gotten this far and a sharded topology doesnt support
    # transactions, something has gone seriously wrong
    if ( $topology eq 'Sharded' && defined $self->session->_recovery_token ) {
        $query->Push( recoveryToken => $self->session->_recovery_token );
    }

    return $query;
}

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $query = $self->_get_query_maybe_with_write_concern( $topology );
    # Set that an attempt to commit the transaction has been made after getting
    # query but before execute - stops error unwind losing it
    $self->session->_has_attempted_end_transaction( 1 );
    my $op = MongoDB::Op::_Command->_new(
        query_flags => {},
        query => $query,
        map { $_ => $self->$_ } qw(db_name bson_codec session monitoring_callback)
    );
    my $result = $op->execute( $link, $topology );
    $result->assert_no_write_concern_error;
    return $result;
}

1;
