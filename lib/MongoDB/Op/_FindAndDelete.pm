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
package MongoDB::Op::_FindAndDelete;

# Encapsulate find_and_delete operation; atomically delete and return doc

use version;
our $VERSION = 'v2.2.2';

use Moo;

use MongoDB::Error;
use MongoDB::Op::_Command;
use Types::Standard qw(
    HashRef
);
use boolean;

use namespace::clean;

has filter => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

has options => (
    is       => 'ro',
    required => 1,
    isa      => HashRef,
);

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_CollectionOp
  MongoDB::Role::_WriteOp
);

sub execute {
    my ( $self, $link, $topology ) = @_;

    if ( defined $self->options->{collation} and !$link->supports_collation ) {
        MongoDB::UsageError->throw(
            "MongoDB host '" . $link->address . "' doesn't support collation" );
    }

    my $command = [
        findAndModify   => $self->coll_name,
        query           => $self->filter,
        remove          => true,
        ($link->supports_find_modify_write_concern ?
            (@{ $self->write_concern->as_args })
            : () ),
        %{ $self->options },
    ];

    my $op = MongoDB::Op::_Command->_new(
        db_name             => $self->db_name,
        query               => $command,
        query_flags         => {},
        bson_codec          => $self->bson_codec,
        session             => $self->session,
        retryable_write     => $self->retryable_write,
        monitoring_callback => $self->monitoring_callback,
    );

    # XXX more special error handling that will be a problem for
    # command monitoring
    my $result;
    eval {
        $result = $op->execute( $link, $topology );
        $result = $result->{output};
        1;
    } or do {
        my $error = $@ || "Unknown error";
        die $error unless $error eq 'No matching object found';
    };

    # findAndModify returns ok:1 even for write concern errors, so
    # we must check and throw explicitly
    if ( $result->{writeConcernError} ) {
        MongoDB::WriteConcernError->throw(
            message => $result->{writeConcernError}{errmsg},
            result  => $result,
            code    => WRITE_CONCERN_ERROR,
        );
    }

    return $result->{value} if $result;
    return;
}

1;
