#
#  Copyright 2015 MongoDB, Inc.
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

package MongoDB::Op::_Aggregate;

# Encapsulate aggregate operation; return MongoDB::QueryResult

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;

use MongoDB::Error;
use MongoDB::Op::_Command;
use MongoDB::_Types -types;
use Types::Standard -types;
use boolean;
use namespace::clean -except => 'meta';

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has coll_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has client => (
    is       => 'ro',
    isa      => InstanceOf ['MongoDB::MongoClient'],
    required => 1,
);

has bson_codec => (
    is       => 'ro',
    isa      => InstanceOf ['MongoDB::MongoClient'], # XXX only for now
    required => 1,
);

has pipeline => (
    is       => 'ro',
    isa      => ArrayOfHashRef,
    required => 1,
);

has options => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

with qw(
  MongoDB::Role::_ReadOp
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $options = $self->options;

    # If 'cursor' is explicitly false, we disable using cursors, even
    # for MongoDB 2.6+.  This allows users operating with a 2.6+ mongos
    # and pre-2.6 mongod in shards to avoid fatal errors.  This
    # workaround should be removed once MongoDB 2.4 is no longer supported.
    my $use_cursor = $link->accepts_wire_version(2)
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

    my @command = (
        aggregate => $self->coll_name,
        pipeline  => $self->pipeline,
        %$options,
    );

    my $op = MongoDB::Op::_Command->new(
        db_name         => $self->db_name,
        query           => Tie::IxHash->new(@command),
        read_preference => $self->read_preference,
    );

    my $res = $op->execute( $link, $topology );

    # For explain, we give the whole response as fields have changed in
    # different server versions
    if ( $options->{explain} ) {
        return MongoDB::QueryResult->new(
            _client => $self->client,
            address => $link->address,
            cursor  => {
                ns         => '',
                id         => 0,
                firstBatch => [ $res->result ],
            },
        );
    }

    # Fake up a single-batch cursor if we didn't get a cursor response.
    # We use the 'results' fields as the first (and only) batch
    if ( !$res->result->{cursor} ) {
        $res->result->{cursor} = {
            ns         => '',
            id         => 0,
            firstBatch => ( delete $res->result->{result} ) || [],
        };
    }

    return $self->_build_result_from_cursor($res);
}

1;
