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

package MongoDB::Op::_Distinct;

# Encapsulate distinct operation; return MongoDB::QueryResult

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;

use MongoDB::Op::_Command;
use MongoDB::_Types -types;
use Types::Standard -types;
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

has fieldname=> (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has filter => (
    is      => 'ro',
    isa     => IxHash,
    coerce  => 1,
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

    my @command = (
        distinct => $self->coll_name,
        key      => $self->fieldname,
        query    => $self->filter,
        %$options
    );

    my $op = MongoDB::Op::_Command->new(
        db_name         => $self->db_name,
        query           => Tie::IxHash->new(@command),
        read_preference => $self->read_preference,
    );

    my $res = $op->execute( $link, $topology );

    $res->result->{cursor} = {
        ns         => '',
        id         => 0,
        firstBatch => ( delete $res->result->{values} ) || [],
    };

    return $self->_build_result_from_cursor($res);
}

1;
