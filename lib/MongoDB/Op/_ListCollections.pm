#
#  Copyright 2014 MongoDB, Inc.
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

package MongoDB::Op::_ListCollections;

# Encapsulate collection list operations; returns arrayref of collection
# names

use version;
our $VERSION = 'v0.999.999.4'; # TRIAL

use Moose;

use MongoDB::Op::_Command;
use MongoDB::Op::_Query;
use MongoDB::QueryResult::Filtered;
use MongoDB::_Types -types;
use Types::Standard -types;
use Tie::IxHash;
use namespace::clean -except => 'meta';

has db_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
);

has client => (
    is       => 'ro',
    isa      => InstanceOf['MongoDB::MongoClient'],
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

with $_ for qw(
  MongoDB::Role::_ReadOp
  MongoDB::Role::_CommandCursorOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    my $res =
        $link->accepts_wire_version(3)
      ? $self->_command_list_colls( $link, $topology )
      : $self->_legacy_list_colls( $link, $topology );

    return $res;
}

sub _command_list_colls {
    my ( $self, $link, $topology ) = @_;

    my $options = $self->options;

    # batchSize is not a command parameter itself like other options
    my $batchSize = delete $options->{batchSize};

    if ( defined $batchSize ) {
        $options->{cursor} = { batchSize => $batchSize };
    }
    else {
        $options->{cursor} = {};
    }

    my $cmd = Tie::IxHash->new(
        listCollections => 1,
        filter => $self->filter,
        %{$self->options},
    );

    my $op = MongoDB::Op::_Command->new(
        db_name         => $self->db_name,
        query           => $cmd,
        read_preference => $self->read_preference,
        bson_codec      => $self->bson_codec,
    );

    my $res = $op->execute( $link, $topology );

    return $self->_build_result_from_cursor( $res );
}

sub _legacy_list_colls {
    my ( $self, $link, $topology ) = @_;

    my $query = MongoDB::_Query->new(
        %{$self->options},
        db_name         => $self->db_name,
        coll_name       => 'system.namespaces',
        bson_codec      => $self->bson_codec,
        client          => $self->client,
        read_preference => $self->read_preference,
        filter          => $self->filter,
    );

    my $op = $query->as_query_op( { post_filter => \&__filter_legacy_names } );

    return $op->execute( $link, $topology );
}

# exclude names with '$' except oplog.$
# XXX why do we include oplog.$?
sub __filter_legacy_names {
    my $doc  = shift;
    # remove leading database name for compatibility with listCollections
    $doc->{name} =~ s/^[^.]+\.//;
    my $name = $doc->{name};
    return !( index( $name, '$' ) >= 0 && index( $name, '.oplog.$' ) < 0 );
}

__PACKAGE__->meta->make_immutable;

1;
