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

package MongoDB::Op::_BatchInsert;

# Encapsulate a multi-document insert operation; returns a
# MongoDB::InsertManyResult

use version;
our $VERSION = 'v1.1.0';

use Moo;

use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::InsertManyResult;
use MongoDB::OID;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use Types::Standard qw(
    Str
    ArrayRef
    Bool
);
use Scalar::Util qw/blessed reftype/;
use Tie::IxHash;
use namespace::clean;

has db_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

has coll_name => (
    is       => 'ro',
    required => 1,
    isa      => Str,
);

# may or may not have _id; will be added if check_keys is true
has documents => (
    is       => 'ro',
    required => 1,
    isa      => ArrayRef,
);

has ordered => (
    is       => 'ro',
    required => 1,
    isa      => Bool,
);

has check_keys => (
    is       => 'ro',
    required => 1,
    isa      => Bool,
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
  MongoDB::Role::_WriteOp
  MongoDB::Role::_InsertPreEncoder
);

sub execute {
    my ( $self, $link ) = @_;

    my $documents = $self->documents;
    my $invalid_chars = $self->check_keys ? '.' : '';

    my (@insert_docs, @ids);

    my $last_idx = $#$documents;
    for ( my $i = 0; $i <= $last_idx; $i++ ) {
        push @insert_docs, $self->_pre_encode_insert( $link, $documents->[$i], $invalid_chars );
        push @ids, $insert_docs[-1]{metadata}{_id};
    }

    $self->_set_doc_ids(\@ids);

    my $res =
        $link->does_write_commands
      ? $self->_command_insert( $link, \@insert_docs )
      : $self->_legacy_op_insert( $link, \@insert_docs );

    $res->assert;
    return $res;
}

sub _command_insert {
    my ( $self, $link, $insert_docs ) = @_;

    # XXX have to check size of docs to insert here and possibly split it

    my $cmd = Tie::IxHash->new(
        insert       => $self->coll_name,
        documents    => $insert_docs,
        @{ $self->write_concern->as_args },
    );

    return $self->_send_write_command( $link, $cmd, undef, "MongoDB::InsertManyResult" );
}

sub _legacy_op_insert {
    my ( $self, $link, $insert_docs ) = @_;

    # XXX have to check size of docs to insert here and possibly split it

    my $ns = $self->db_name . "." . $self->coll_name;
    my $op_bson =
      MongoDB::_Protocol::write_insert( $ns, join( "", map { $_->{bson} } @$insert_docs ) );

    return $self->_send_legacy_op_with_gle( $link, $op_bson, undef,
        "MongoDB::InsertManyResult" );
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
