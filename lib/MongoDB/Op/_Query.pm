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

package MongoDB::Op::_Query;

# Encapsulate a query operation; returns a MongoDB::QueryResult object

use version;
our $VERSION = 'v0.999.999.3'; # TRIAL

use Moose;

use MongoDB::BSON;
use MongoDB::QueryResult;
use MongoDB::_Protocol;
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
    isa      => InstanceOf['MongoDB::MongoClient'],
    required => 1,
);

has query => (
    is       => 'ro',
    isa      => IxHash,
    coerce   => 1,
    required => 1,
    writer   => '_set_query',
);

has projection => (
    is     => 'ro',
    isa    => IxHash,
    coerce => 1,
);

has [qw/batch_size limit skip/] => (
    is      => 'ro',
    isa     => Num,
    default => 0,
);

# XXX eventually make this a hash with restricted keys?
has query_flags => (
    is      => 'ro',
    isa     => HashRef,
    default => sub { {} },
);

has post_filter => (
    is        => 'ro',
    isa       => Maybe[CodeRef],
    predicate => 'has_post_filter',
);

with 'MongoDB::Role::_ReadOp';
with 'MongoDB::Role::_ReadPrefModifier';

sub execute {
    my ( $self, $link, $topology_type ) = @_;

    my $ns         = $self->db_name . "." . $self->coll_name;
    my $filter     = $self->bson_codec->encode_one( $self->query );
    my $batch_size = $self->limit || $self->batch_size;            # limit trumps

    my $proj =
      $self->projection ? $self->bson_codec->encode_one( $self->projection ) : undef;

    $self->_apply_read_prefs( $link, $topology_type );

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $ns, $filter, $proj, $self->skip, $batch_size,
        $self->query_flags );

    my $result =
      $self->_query_and_receive( $link, $op_bson, $request_id, $self->bson_codec );

    my $class =
      $self->has_post_filter ? "MongoDB::QueryResult::Filtered" : "MongoDB::QueryResult";

    return $class->new(
        _client     => $self->client,
        bson_codec  => $self->bson_codec,
        address     => $link->address,
        ns          => $ns,
        limit       => $self->limit,
        batch_size  => $batch_size,
        reply       => $result,
        post_filter => $self->post_filter,
    );
}

1;
