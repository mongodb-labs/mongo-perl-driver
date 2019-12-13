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
package MongoDB::Op::_BatchInsert;

# Encapsulate a multi-document insert operation; returns a
# MongoDB::InsertManyResult

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::InsertManyResult;
use Tie::IxHash;
use MongoDB::_Types qw(
    Boolish
);
use Types::Standard qw(
    ArrayRef
);

use namespace::clean;

# may or may not have _id; will be added if check_keys is true
has documents => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef,
);

has ordered => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

has check_keys => (
    is       => 'ro',
    required => 1,
    isa      => Boolish,
);

# starts empty and gets initialized during operations
has _doc_ids => (
    is       => 'ro',
    writer   => '_set_doc_ids',
    init_arg => undef,
    isa      => ArrayRef,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_SingleBatchDocWrite
  MongoDB::Role::_InsertPreEncoder
);

sub execute {
    my ( $self, $link ) = @_;

    my $documents = $self->documents;
    my $invalid_chars = $self->check_keys ? '.' : '';

    my (@insert_docs, @ids);

    my $last_idx = $#$documents;
    for ( my $i = 0; $i <= $last_idx; $i++ ) {
        push @insert_docs, $self->_pre_encode_insert( $link->max_bson_object_size, $documents->[$i], $invalid_chars );
        push @ids, $insert_docs[-1]{metadata}{_id};
    }

    $self->_set_doc_ids(\@ids);

    # XXX have to check size of docs to insert and possibly split it
    #
    return ! $self->_should_use_acknowledged_write
      ? (
        $self->_send_legacy_op_noreply( $link,
            MongoDB::_Protocol::write_insert( $self->full_name, join( "", map { $_->{bson} } @insert_docs ) ),
            \@insert_docs,
            "MongoDB::UnacknowledgedResult",
            "insert",
        )
      )
      : $link->supports_write_commands
      ? (
        $self->_send_write_command( $link,
            Tie::IxHash->new(
                insert       => $self->coll_name,
                documents    => \@insert_docs,
                @{ $self->write_concern->as_args },
            ),
            undef,
            "MongoDB::InsertManyResult",
        )->assert
      )
      : (
        $self->_send_legacy_op_with_gle( $link,
            MongoDB::_Protocol::write_insert( $self->full_name, join( "", map { $_->{bson} } @insert_docs ) ),
            \@insert_docs,
            "MongoDB::InsertManyResult",
            "insert",
        )->assert
      );
}

sub _parse_cmd {
    my ( $self, $res ) = @_;
    return unless $res->{ok};
    my $inserted = $self->_doc_ids;
    my $ids = [ map +{ index => $_,  _id => $inserted->[$_] }, 0 .. $#{$inserted} ];
    return ( inserted_count => scalar @$inserted, inserted => $ids );
}

BEGIN {
    no warnings 'once';
    *_parse_gle = \&_parse_cmd;
}

1;
