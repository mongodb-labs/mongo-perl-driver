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
package MongoDB::Op::_Command;

# Encapsulate running a command and returning a MongoDB::CommandResult

use version;
our $VERSION = 'v1.999.0';

use Moo;

use MongoDB::_Constants;
use MongoDB::_Types qw(
    Document
    ReadPreference
);
use Types::Standard qw(
    HashRef
    Maybe
    InstanceOf
);

use namespace::clean;

has query => (
    is       => 'ro',
    required => 1,
    writer   => '_set_query',
    isa      => Document,
);

has query_flags => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

has read_preference => (
    is  => 'ro',
    isa => Maybe [ReadPreference],
);

has client => (
    is => 'ro',
    required => 0,
    isa => Maybe [InstanceOf['MongoDB::MongoClient']],
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseOp
  MongoDB::Role::_ReadPrefModifier
  MongoDB::Role::_ClusterTimeModifier
);

sub execute {
    my ( $self, $link, $topology_type ) = @_;
    $topology_type ||= 'Single'; # if not specified, assume direct

    $self->_apply_session( \$self->{query} );
    $self->_apply_cluster_time( $link, \$self->{query} );

    # $query is passed as a reference because it *may* be replaced
    $self->_apply_read_prefs( $link, $topology_type, $self->{query_flags}, \$self->{query});

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $self->{db_name} . '.$cmd',
        $self->{bson_codec}->encode_one( $self->{query} ), undef, 0, -1, $self->{query_flags});

    if ( length($op_bson) > MAX_BSON_WIRE_SIZE ) {
        # XXX should this become public?
        MongoDB::_CommandSizeError->throw(
            message => "database command too large",
            size    => length $op_bson,
        );
    }

    $link->write( $op_bson ),
    ( my $result = MongoDB::_Protocol::parse_reply( $link->read, $request_id ) );

    my $res = MongoDB::CommandResult->_new(
        output => $self->{bson_codec}->decode_one( $result->{docs} ),
        address => $link->address,
    );

    $res->assert;

    $self->_read_cluster_time($res);
    $self->_retire_implicit_session;

    return $res;
}

1;
