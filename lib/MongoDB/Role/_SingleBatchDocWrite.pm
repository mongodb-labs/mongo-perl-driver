#  Copyright 2016 - present MongoDB, Inc.
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
package MongoDB::Role::_SingleBatchDocWrite;

# MongoDB interface for database insert/update/delete operations

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::CommandResult;
use MongoDB::Error;
use MongoDB::UnacknowledgedResult;
use MongoDB::_Constants;
use MongoDB::_Protocol;
use MongoDB::_Types qw(
    WriteConcern
    to_IxHash
);

use namespace::clean;

with $_ for qw(
  MongoDB::Role::_WriteOp
  MongoDB::Role::_SessionSupport
  MongoDB::Role::_CommandMonitoring
);

requires qw/db_name write_concern _parse_cmd _parse_gle/;

sub _send_legacy_op_with_gle {
    my ( $self, $link, $op_bson, $request_id, $op_doc, $result_class, $cmd_name ) = @_;

    my $wc_args = $self->write_concern->as_args();
    my @write_concern = scalar @$wc_args ? %{ $wc_args->[1] } : ();

    my $gle = $self->bson_codec->encode_one( [ getlasterror => 1, @write_concern ] );
    my ( $gle_bson, $gle_request_id ) =
        MongoDB::_Protocol::write_query( $self->db_name . '.$cmd', $gle, undef, 0, -1 );

    # write op sent as a unit with GLE command to ensure GLE applies to the
    # operation without other operations in between
    $op_bson .= $gle_bson;

    if ( length($op_bson) > MAX_BSON_WIRE_SIZE ) {
        # XXX should this become public?
        MongoDB::_CommandSizeError->throw(
            message => "database command too large",
            size    => length $op_bson,
        );
    }

    $self->publish_legacy_write_started( $link, $cmd_name, $op_doc, $request_id )
      if $self->monitoring_callback;

    my $result;
    eval {
        $link->write( $op_bson ),
        ( $result = MongoDB::_Protocol::parse_reply( $link->read, $gle_request_id ) );
    };
    if ( my $err = $@ ) {
        $self->publish_command_exception($err) if $self->monitoring_callback;
        die $err;
    }

    $self->publish_command_reply( $result->{docs} )
      if $self->monitoring_callback;

    my $res = $self->bson_codec->decode_one( $result->{docs} );

    # errors in the command itself get handled as normal CommandResult
    if ( !$res->{ok} && ( $res->{errmsg} || $res->{'$err'} ) ) {
        return MongoDB::CommandResult->_new(
            output  => $res,
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
            result => MongoDB::CommandResult->_new(
                output => $res,
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

    return $result_class->_new(
        acknowledged         => 1,
        write_errors         => ( $write_error ? [$write_error] : [] ),
        write_concern_errors => ( $write_concern_error ? [$write_concern_error] : [] ),
        $self->_parse_gle( $res, $op_doc ),
    );
}

sub _send_legacy_op_noreply {
    my ( $self, $link, $op_bson, $request_id, $op_doc, $result_class, $cmd_name) = @_;

    $self->publish_legacy_write_started( $link, $cmd_name, $op_doc, $request_id )
      if $self->monitoring_callback;

    eval { $link->write($op_bson) };
    if ( my $err = $@ ) {
        $self->publish_command_exception($err) if $self->monitoring_callback;
        die $err;
    }

    $self->publish_command_reply( { ok => 1 } )
      if $self->monitoring_callback;

    return MongoDB::UnacknowledgedResult->_new(
        $self->_parse_gle( {}, $op_doc ),
        acknowledged         => 0,
        write_errors         => [],
        write_concern_errors => [],
    );
}

sub _send_write_command {
    my ( $self, $link, $cmd, $op_doc, $result_class ) = @_;

    $self->_apply_session_and_cluster_time( $link, \$cmd );

    my ( $op_bson, $request_id );
    if ( $link->supports_op_msg ) {
        $cmd = to_IxHash( $cmd );
        $cmd->Push( '$db', $self->db_name );
        ( $op_bson, $request_id ) =
            MongoDB::_Protocol::write_msg( $self->bson_codec, undef, $cmd );
    } else {
        # send command and get response document
        my $command = $self->bson_codec->encode_one( $cmd );
        ( $op_bson, $request_id ) =
          MongoDB::_Protocol::write_query( $self->db_name . '.$cmd',
            $command, undef, 0, -1, undef );
    }

    if ( length($op_bson) > MAX_BSON_WIRE_SIZE ) {
        # XXX should this become public?
        MongoDB::_CommandSizeError->throw(
            message => "database command too large",
            size    => length $op_bson,
        );
    }

    $self->publish_command_started( $link, $cmd, $request_id )
      if $self->monitoring_callback;

    my $result;
    eval {
        $link->write( $op_bson ),
        ( $result = MongoDB::_Protocol::parse_reply( $link->read, $request_id ) );
    };
    if ( my $err = $@ ) {
        $self->_update_session_connection_error( $err );
        $self->publish_command_exception($err) if $self->monitoring_callback;
        die $err;
    }

    $self->publish_command_reply( $result->{docs} )
      if $self->monitoring_callback;

    my $res = $self->bson_codec->decode_one( $result->{docs} );

    $self->_update_session_pre_assert( $res );

    $self->_update_session_and_cluster_time($res);

    # Error checking depends on write concern
    if ( $self->_should_use_acknowledged_write ) {
        # errors in the command itself get handled as normal CommandResult
        if ( !$res->{ok} && ( $res->{errmsg} || $res->{'$err'} ) ) {
            return MongoDB::CommandResult->_new(
                output => $res,
                address => $link->address,
                session => $self->session,
            );
        }

        # if an error occurred, add the op document involved
        if ( exists($res->{writeErrors}) && @{$res->{writeErrors}} ) {
            $res->{writeErrors}[0]{op} = $op_doc;
        }

        # otherwise, construct the desired result object, calling back
        # on class-specific parser to generate additional attributes
        my $built_result = $result_class->_new(
            write_errors => ( $res->{writeErrors} ? $res->{writeErrors} : [] ),
            write_concern_errors =>
              ( $res->{writeConcernError} ? [ $res->{writeConcernError} ] : [] ),
            $self->_parse_cmd($res),
        );
        $self->_assert_session_errors( $built_result );
        return $built_result;
    }
    else {
        return MongoDB::UnacknowledgedResult->_new(
            write_errors => [],
            write_concern_errors => [],
        );
    }
}

1;
