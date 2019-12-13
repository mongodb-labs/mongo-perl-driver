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
package MongoDB::Op::_Aggregate;

# Encapsulate aggregate operation; return MongoDB::QueryResult

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::_Types qw(
    ArrayOfHashRef
    Boolish
);
use Types::Standard qw(
    HashRef
    InstanceOf
    Num
);

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa      => InstanceOf ['MongoDB::MongoClient'],
);

has pipeline => (
    is       => 'ro',
    required => 1,
    isa      => ArrayOfHashRef,
);

has options => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

has has_out => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has maxAwaitTimeMS => (
    is       => 'rw',
    isa      => Num,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_ReadOp
  MongoDB::Role::_WriteOp
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $options = $self->options;
    my $is_2_6 = $link->supports_write_commands;

    # maxTimeMS isn't available until 2.6 and the aggregate command
    # will reject it as unrecognized
    delete $options->{maxTimeMS} unless $is_2_6;

    # bypassDocumentValidation isn't available until 3.2 (wire version 4) & dont send if false
    unless ($link->supports_document_validation && $options->{bypassDocumentValidation}) {
        delete $options->{bypassDocumentValidation};
    }

    if ( defined $options->{collation} and !$link->supports_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" );
    }

    # If 'cursor' is explicitly false, we disable using cursors, even
    # for MongoDB 2.6+.  This allows users operating with a 2.6+ mongos
    # and pre-2.6 mongod in shards to avoid fatal errors.  This
    # workaround should be removed once MongoDB 2.4 is no longer supported.
    my $use_cursor = $is_2_6
      && ( !exists( $options->{cursor} ) || $options->{cursor} );

    # batchSize is not a command parameter itself like other options
    my $batchSize = delete $options->{batchSize};

    # If we're doing cursors, we first respect an explicit batchSize option;
    # next we fallback to the legacy (deprecated) cursor option batchSize; finally we
    # just give an empty document. Other than batchSize we ignore any other
    # legacy cursor options.  If we're not doing cursors, don't send any
    # cursor option at all, as servers will choke on it.
    if ($use_cursor) {
        if ( defined $batchSize ) {
            $options->{cursor} = { batchSize => $batchSize };
        }
        elsif ( ref $options->{cursor} eq 'HASH' ) {
            $batchSize = $options->{cursor}{batchSize};
            $options->{cursor} = defined($batchSize) ? { batchSize => $batchSize } : {};
        }
        else {
            $options->{cursor} = {};
        }
    }
    else {
        delete $options->{cursor};
    }

    my $has_out = $self->has_out;

    if ( $self->coll_name eq 1 && ! $link->supports_db_aggregation ) {
        MongoDB::Error->throw(
            "Calling aggregate with a collection name of '1' is not supported on Wire Version < 6" );
    }

    my @command = (
        aggregate => $self->coll_name,
        pipeline  => $self->pipeline,
        %$options,
        (
            $link->supports_aggregate_out_read_concern || (!$has_out && $link->supports_read_concern) ?
                @{ $self->read_concern->as_args( $self->session) } : ()
        ),
        (
            $has_out && $link->supports_helper_write_concern ? @{ $self->write_concern->as_args } : ()
        ),
    );

    my $op = MongoDB::Op::_Command->_new(
        db_name     => $self->db_name,
        query       => Tie::IxHash->new(@command),
        query_flags => {},
        bson_codec  => $self->bson_codec,
        ( $has_out ? () : ( read_preference => $self->read_preference ) ),
        session             => $self->session,
        monitoring_callback => $self->monitoring_callback,
    );

    my $res = $op->execute( $link, $topology );

    $res->assert_no_write_concern_error if $has_out;

    # For explain, we give the whole response as fields have changed in
    # different server versions
    if ( $options->{explain} ) {
        return MongoDB::QueryResult->_new(
            _client       => $self->client,
            _address      => $link->address,
            _full_name    => '',
            _bson_codec   => $self->bson_codec,
            _batch_size   => 1,
            _cursor_at    => 0,
            _limit        => 0,
            _cursor_id    => 0,
            _cursor_start => 0,
            _cursor_flags => {},
            _cursor_num   => 1,
            _docs         => [ $res->output ],
        );
    }

    # Fake up a single-batch cursor if we didn't get a cursor response.
    # We use the 'results' fields as the first (and only) batch
    if ( !$res->output->{cursor} ) {
        $res->output->{cursor} = {
            ns         => '',
            id         => 0,
            firstBatch => ( delete $res->output->{result} ) || [],
        };
    }

    return $self->_build_result_from_cursor($res);
}

1;
