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

package MongoDB::Op::_Update;

# Encapsulate an update operation; returns a MongoDB::UpdateResult

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;

use MongoDB::BSON;
use MongoDB::UpdateResult;
use MongoDB::_Protocol;
use MongoDB::_Types -types;
use Types::Standard -types;
use Tie::IxHash;
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

has filter => (
    is       => 'ro',
    isa      => IxHash,
    coerce   => 1,
    required => 1,
);

has update => (
    is       => 'ro',
    isa      => IxHash,
    coerce   => 1,
    required => 1,
);

has multi => (
    is     => 'ro',
    isa    => Bool,
);

has upsert => (
    is     => 'ro',
    isa    => Bool,
);

with qw/MongoDB::Role::_WriteOp/;

sub execute {
    my ( $self, $link ) = @_;

    my $update_op = {
        q      => $self->filter,
        u      => $self->update,
        multi  => boolean($self->multi),
        upsert => boolean($self->upsert),
    };

    # XXX until we have a proper BSON::Raw class, we bless on the fly
    my $first_key  = $update_op->{u}->Keys(0);
    my $is_replace = substr( $first_key, 0, 1 ) ne '$';
    my $max_size   = $is_replace ? $link->max_bson_object_size : undef;
    my $bson_doc = MongoDB::BSON::encode_bson( $update_op->{u}, $is_replace, $max_size );
    my $orig_op = { %$update_op };
    $update_op->{u} = bless \$bson_doc, "MongoDB::BSON::Raw";

    my $res =
        $link->accepts_wire_version(2)
      ? $self->_command_update( $link, $update_op, $orig_op )
      : $self->_legacy_op_update( $link, $update_op, $orig_op );

    $res->assert;
    return $res;
}

sub _command_update {
    my ( $self, $link, $op_doc, $orig_doc ) = @_;

    my $cmd = Tie::IxHash->new(
        update       => $self->coll_name,
        updates      => [$op_doc],
        writeConcern => $self->write_concern->as_struct,
    );

    return $self->_send_write_command( $link, $cmd, $orig_doc, "MongoDB::UpdateResult" );
}

sub _legacy_op_update {
    my ( $self, $link, $op_doc, $orig_doc ) = @_;

    my $flags = {};
    @{$flags}{qw/upsert multi/} = @{$op_doc}{qw/upsert multi/};

    my $ns          = $self->db_name . "." . $self->coll_name;
    my $query_bson  = MongoDB::BSON::encode_bson( $op_doc->{q}, 0 );
    my $update_bson = ${ $op_doc->{u} };                            # already raw BSON
    my $op_bson =
      MongoDB::_Protocol::write_update( $ns, $query_bson, $update_bson, $flags );

    return $self->_send_legacy_op_with_gle( $link, $op_bson, $orig_doc, "MongoDB::UpdateResult" );
}

sub _parse_cmd {
    my ( $self, $res ) = @_;

    return (
        matched_count  => $res->{n} - @{ $res->{upserted} || [] },
        modified_count => $res->{nModified},
        upserted_id    => $res->{upserted} ? $res->{upserted}[0]{_id} : undef,
    );
}

sub _parse_gle {
    my ( $self, $res, $orig_doc ) = @_;

    # For 2.4 and earlier, 'upserted' has _id only if the _id is an OID.
    # Otherwise, we have to pick it out of the update document or query
    # document when we see updateExisting is false but the number of docs
    # affected is 1

    my $upserted = $res->{upserted};
    if (! defined( $upserted )
        && exists( $res->{updatedExisting} )
        && !$res->{updatedExisting}
        && $res->{n} == 1 )
    {
        $upserted =
            $orig_doc->{u}->EXISTS("_id")
          ? $orig_doc->{u}->FETCH("_id")
          : $orig_doc->{q}->FETCH("_id");
    }

    return (
        matched_count  => ($upserted ? 0 : $res->{n}),
        modified_count => undef,
        upserted_id    => $upserted,
    );
}

1;
