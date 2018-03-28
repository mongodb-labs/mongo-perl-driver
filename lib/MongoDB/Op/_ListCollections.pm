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

use strict;
use warnings;
package MongoDB::Op::_ListCollections;

# Encapsulate collection list operations; returns arrayref of collection
# names

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::Op::_Command;
use MongoDB::Op::_Query;
use MongoDB::ReadConcern;
use MongoDB::ReadPreference;
use MongoDB::_Types qw(
    Document
);
use Types::Standard qw(
    HashRef
    InstanceOf
    Str
);
use Tie::IxHash;

use namespace::clean;

has client => (
    is       => 'ro',
    required => 1,
    isa => InstanceOf['MongoDB::MongoClient'],
);

has filter => (
    is      => 'ro',
    required => 1,
    isa => Document,
);

has options => (
    is      => 'ro',
    required => 1,
    isa => HashRef,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseOp
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

    my $filter =
      ref( $self->filter ) eq 'ARRAY'
      ? { @{ $self->filter } }
      : $self->filter;

    my $cmd = Tie::IxHash->new(
        listCollections => 1,
        filter => $filter,
        %{$self->options},
    );

    my $op = MongoDB::Op::_Command->_new(
        db_name         => $self->db_name,
        query           => $cmd,
        query_flags     => {},
        bson_codec      => $self->bson_codec,
        session         => $self->session,
    );

    my $res = $op->execute( $link, $topology );

    return $self->_build_result_from_cursor( $res );
}

sub _legacy_list_colls {
    my ( $self, $link, $topology ) = @_;

    my $op = MongoDB::Op::_Query->_new(
        modifiers           => {},
        allowPartialResults => 0,
        batchSize           => 0,
        comment             => '',
        cursorType          => 'non_tailable',
        limit               => 0,
        maxAwaitTimeMS      => 0,
        maxTimeMS           => 0,
        noCursorTimeout     => 0,
        oplogReplay         => 0,
        projection          => undef,
        skip                => 0,
        sort                => undef,
        %{$self->options},
        db_name         => $self->db_name,
        coll_name       => 'system.namespaces',
        full_name       => $self->db_name . ".system.namespaces",
        bson_codec      => $self->bson_codec,
        client          => $self->client,
        read_preference => MongoDB::ReadPreference->new,
        read_concern    => MongoDB::ReadConcern->new,
        filter          => $self->filter,
        post_filter => \&__filter_legacy_names
    );

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

1;
