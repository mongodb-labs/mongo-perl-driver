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

package MongoDB::Role::_WriteOp;

# MongoDB interface for database write operations

use version;
our $VERSION = 'v0.999.998.2'; # TRIAL

use MongoDB::BSON;
use MongoDB::CommandResult;
use MongoDB::Error;
use MongoDB::_Protocol;
use MongoDB::_Types;
use Moose::Role;
use Syntax::Keyword::Junction qw/any/;
use namespace::clean -except => 'meta';

use constant {
    NO_JOURNAL_RE => qr/^journaling not enabled/,
    NO_REPLICATION_RE => qr/^no replication has been enabled/,
};

with qw/MongoDB::Role::_CommandOp/;

requires qw/db_name _parse_cmd _parse_gle/;

has write_concern => (
    is       => 'ro',
    isa      => 'WriteConcern',
    required => 1,
);

sub _send_legacy_op_with_gle {
    my ( $self, $link, $op_bson, $op_doc, $result_class ) = @_;

    if ( $self->write_concern->is_safe ) {
        my @write_concern = %{ $self->write_concern->as_struct };
        my $gle = MongoDB::BSON::encode_bson( [ getlasterror => 1, @write_concern ], 0 );
        my ( $gle_bson, $request_id ) =
          MongoDB::_Protocol::write_query( $self->db_name . '.$cmd', $gle, undef, 0, -1 );

        # write op sent as a unit with GLE command to ensure GLE applies to the
        # operation without other operations in between
        my $res =
          $self->_query_and_receive( $link, $op_bson . $gle_bson, $request_id, undef )
          ->{docs}[0];


        # errors in the command itself get handled as normal CommandResult
        if ( !$res->{ok} && ( $res->{errmsg} || $res->{'$err'} ) ) {
            return MongoDB::CommandResult->new(
                result  => $res,
                address => $link->address,
            );
        }

        # 'ok' false means GLE itself failed
        # usually we shouldn't check wnote or jnote, but the Bulk API QA test says we should
        # detect no journal or replication not enabled, so we check for special strings.
        # These strings were checked back to MongoDB 1.8.5.
        my $got_error =
          ( exists( $res->{jnote} ) && $res->{jnote} =~ NO_JOURNAL_RE )     ? $res->{jnote}
        : ( exists( $res->{wnote} ) && $res->{wnote} =~ NO_REPLICATION_RE ) ? $res->{wnote}
        :                                                                     undef;

        if ($got_error) {
            MongoDB::DatabaseError->throw(
                message => $got_error,
                result => MongoDB::CommandResult->new(
                    result => $res,
                    address => $link->address,
                ),
            );
        }

        # otherwise, construct the desired result object, calling back
        # on class-specific parser to generate additional attributes
        my ( $write_concern_error, $write_error );
        my $errmsg   = $res->{err};
        my $wtimeout = $res->{wtimeout};

        if ($wtimeout) {
            $write_concern_error = {
                errmsg  => $errmsg,
                errInfo => { wtimeout => $wtimeout },
                code    => $res->{code} || WRITE_CONCERN_ERROR,
            };
        }
        elsif ($errmsg) {
            $write_error = {
                errmsg => $errmsg,
                code   => $res->{code} || UNKNOWN_ERROR,
                index  => 0,
                op     => $op_doc,
            };
        }

        return $result_class->new(
            ( $write_error ? ( write_errors => [$write_error] ) : () ),
            (
                $write_concern_error
                ? ( write_concern_errors => [$write_concern_error] )
                : ()
            ),
            $self->_parse_gle($res, $op_doc),
        );
    }
    else {
        $link->write($op_bson);
        return $result_class->new( acknowledged => 0 );
    }
}

sub _send_write_command {
    my ( $self, $link, $cmd, $op_doc, $result_class ) = @_;

    my $res = $self->_send_command( $link, $cmd );

    if ( $self->write_concern->is_safe ) {
        # errors in the command itself get handled as normal CommandResult
        if ( !$res->{ok} && ( $res->{errmsg} || $res->{'$err'} ) ) {
            return MongoDB::CommandResult->new(
                result  => $res,
                address => $link->address,
            );
        }

        # if an error occurred, add the op document involved
        if ( exists($res->{writeErrors}) && @{$res->{writeErrors}} ) {
            $res->{writeErrors}[0]{op} = $op_doc;
        }

        # otherwise, construct the desired result object, calling back
        # on class-specific parser to generate additional attributes
        return $result_class->new(
            ( $res->{writeErrors} ? ( write_errors => $res->{writeErrors} ) : () ),
            (
                $res->{writeConcernError}
                ? ( write_concern_errors => $res->{writeConcernError} )
                : ()
            ),
            $self->_parse_cmd($res),
        );
    }
    else {
        return $result_class->new( acknowledged => 0 );
    }
}

1;
