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

package MongoDB::Role::_Client;

# Role implementing database operations over a socket; includes BSON
# transformations where appropriate

use version;
our $VERSION = 'v0.999.998.2'; # TRIAL

use MongoDB::BSON;
use MongoDB::Error;
use MongoDB::_Protocol;
use MongoDB::_Types;
use Moose::Role;
use namespace::clean -except => 'meta';

use constant {
    MAX_BSON_WIRE_SIZE => 16_793_600, # 16MiB + 16KiB
    NO_JOURNAL_RE => qr/^journaling not enabled/,
    NO_REPLICATION_RE => qr/^no replication has been enabled/,
};

# returns MongoDB::CommandResult
sub _send_admin_command {
    my ( $self, $link, $args ) = @_;
    $args->{db} = 'admin';
    return $self->_send_command( $link, $args );
}

# returns MongoDB::CommandResult
sub _send_command {
    my ( $self, $link, $args ) = @_;

    my $command = MongoDB::BSON::encode_bson( $args->{command}->filter, 0 );

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $args->{db} . '.$cmd', $command, undef, 0, -1, $args->{flags} );

    if ( length($op_bson) > MAX_BSON_WIRE_SIZE ) {
        # XXX should this become public?
        MongoDB::_CommandSizeError->throw(
            message => "database command too large",
            size => length $op_bson,
        );
    }

    my $result = $self->_query_and_receive( $link, $op_bson, $request_id, undef, 1 );

    $result = MongoDB::CommandResult->new(
        result => $result->{docs}[0],
        address => $link->address
    );

    $result->assert;

    return $result;
}

# Returns MongoDB::WriteResult
sub _send_delete {
    my ( $self, $link, $args ) = @_;
    # $op_doc is { q: $query, limit: $limit }
    my $op_doc = $args->{op_doc};

    # XXX eventually, based on link metadata about server wire protocol version
    # this is where we should choose a write command or a legacy op; the legacy
    # op code follows

    my $flags = {
        just_one => (defined( $op_doc->{limit} ) && $op_doc->{limit} == 1) ? 1 : 0,
    };

    my $query_bson  = MongoDB::BSON::encode_bson( $op_doc->{q},  0 );
    my $op_bson = MongoDB::_Protocol::write_delete( $args->{ns}, $query_bson, $flags );

    return $self->_write_legacy_op( "delete", $link, $op_bson, $op_doc, $args );
}

# returns a hashref with fields: response_flags, cursor_id, starting_from, number_returned, docs, address
sub _send_get_more {
    my ( $self, $link, $args ) = @_;

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_get_more( @{$args}{qw/ns cursor_id batch_size/} );

    my $result = $self->_query_and_receive( $link, $op_bson, $request_id, $args->{client}, 0 );
    $result->{address} = $link->address;
    return $result;
}

# Returns MongoDB::WriteResult
sub _send_insert {
    my ( $self, $link, $args ) = @_;
    # $op_doc is document to insert
    my $op_doc = $args->{op_doc};

    # XXX eventually, based on link metadata about server wire protocol version
    # this is where we should choose a write command or a legacy op; the legacy
    # op code follows

    my $check_keys = $args->{check_keys};
    my $max_size = $link->max_bson_object_size;
    my $doc_bson = MongoDB::BSON::encode_bson( $op_doc, $check_keys, $max_size );
    my $op_bson = MongoDB::_Protocol::write_insert( $args->{ns}, $doc_bson, $args->{flags} );

    return $self->_write_legacy_op( "insert", $link, $op_bson, $op_doc, $args );
}

# Returns MongoDB::WriteResult
sub _send_insert_batch {
    my ( $self, $link, $args ) = @_;
    # $op_doc is array ref of documents to insert
    my $op_doc = $args->{op_doc};

    # XXX eventually, based on link metadata about server wire protocol version
    # this is where we should choose a write command or a legacy op; the legacy
    # op code follows

    my $check_keys = $args->{check_keys};
    my $max_size   = $link->max_bson_object_size;
    my $docs_bson  = join( "",
        map { MongoDB::BSON::encode_bson( $_, $check_keys, $max_size ) } @$op_doc );
    my $op_bson =
      MongoDB::_Protocol::write_insert( $args->{ns}, $docs_bson, $args->{flags} );

    return $self->_write_legacy_op( "insert", $link, $op_bson, undef, $args );
}

# returns nothing
sub _send_kill_cursors {
    my ( $self, $link, @cursors ) = @_;

    $link->write( MongoDB::_Protocol::write_kill_cursors(@cursors) );

    return;
}

# Returns MongoDB::WriteResult
sub _send_update {
    my ( $self, $link, $args ) = @_;

    # $op_doc is { q: $query, u: $update, multi: $multi, upsert: $upsert }
    my $op_doc = $args->{op_doc};

    # XXX eventually, based on link metadata about server wire protocol version
    # this is where we should choose a write command or a legacy op; the legacy
    # op code follows

    my $flags = {
        upsert => $op_doc->{upsert},
        multi  => $op_doc->{multi},
    };

    my $update = $op_doc->{u};
    my $type = ref $update;
    my $first_key =
        $type eq 'ARRAY' ? $update->[0]
      : $type eq 'HASH'  ? each %$update
      :                    $update->Keys(0);

    my $is_replace = substr( $first_key, 0, 1 ) ne '$';

    my $max_size = $is_replace ? $link->max_bson_object_size : undef;

    my $query_bson  = MongoDB::BSON::encode_bson( $op_doc->{q},  0 );
    my $update_bson = MongoDB::BSON::encode_bson( $update, $is_replace, $max_size );
    my $op_bson = MongoDB::_Protocol::write_update( $args->{ns}, $query_bson, $update_bson, $flags );

    return $self->_write_legacy_op( "update", $link, $op_bson, $op_doc, $args );
}

# returns a QueryResult
sub _send_query {
    my ( $self, $link, $ns, $query, $fields, $skip, $limit, $size, $flags, $client ) = @_;

    $query = MongoDB::BSON::encode_bson( $query, 0 );
    $fields = MongoDB::BSON::encode_bson( $fields, 0 ) if $fields;

    my ( $op_bson, $request_id ) =
      MongoDB::_Protocol::write_query( $ns, $query, $fields, $skip, $limit || $size, $flags );

    my $result = $self->_query_and_receive( $link, $op_bson, $request_id, $client, 0 );

    return MongoDB::QueryResult->new(
        _client    => $self,
        address    => $link->address,
        ns         => $ns,
        limit      => $limit,
        batch_size => $limit || $size,
        result     => $result,
    );
}

sub _write_legacy_op {
    my ( $self, $type, $link, $op_bson, $op_doc, $args ) = @_;

    my $write_concern = $args->{write_concern};

    if ( ! $write_concern || ! $write_concern->is_safe ) {
        $link->write($op_bson);
        # fake a w=0 write result
        return  MongoDB::WriteResult->_parse(
            op       => $type,
            op_count => 1,
            result   => { n => 0 },
        );
    }

    my ($db_name) = $args->{ns} =~ /^([^.]+)/;
    my @write_concern = %{ $write_concern->as_struct };
    my $gle = MongoDB::BSON::encode_bson( [ getlasterror => 1, @write_concern ], 0 );
    my ( $gle_bson, $request_id ) =
      MongoDB::_Protocol::write_query( "$db_name\.\$cmd", $gle, undef, 0, -1 );

    my $gle_result =
      $self->_query_and_receive( $link, $op_bson . $gle_bson, $request_id, undef, 1 );

    return $self->_writeresult_from_gle( $link, $type, $gle_result->{docs}[0], $op_doc );
}


# XXX expands docs field; uses client for DBRef expansion, which should be abstracted somehow later
sub _query_and_receive {
    my ( $self, $link, $op_bson, $request_id, $client, $is_cmd ) = @_;

    $link->write($op_bson);
    my $result = MongoDB::_Protocol::parse_reply( $link->read, $request_id );

    if ( $result->{flags}{cursor_not_found} ) {
        MongoDB::CursorNotFoundError->throw("cursor not found");
    }

    my $doc_bson = $result->{docs};

    my @documents;
    # XXX eventually, BSON needs an API to do this efficiently for us without a loop here
    for ( 1 .. $result->{number_returned} ) {
        my $len = unpack( MongoDB::_Protocol::P_INT32(), substr( $doc_bson, 0, 4 ) );
        if ( $len > length($doc_bson) ) {
            MongoDB::ProtocolError->throw("document in response was truncated");
        }
        push @documents,
          MongoDB::BSON::decode_bson( substr( $doc_bson, 0, $len, '' ), $client );
    }

    if ( @documents != $result->{number_returned} ) {
        MongoDB::ProtocolError->throw("unexpected number of documents");
    }

    if ( length($doc_bson) > 0 ) {
        MongoDB::ProtocolError->throw("unexpected extra data in response");
    }

    $result->{docs} = \@documents;

    if ( $result->{flags}{query_failure} && !$is_cmd ) {
        # pretend the query was a command and assert it here
        MongoDB::CommandResult->new(
            result  => $result->{docs}[0],
            address => $link->{address}
        )->assert;
    }

    return $result;
}

sub _writeresult_from_gle {
    my ( $self, $link, $type, $gle, $doc ) = @_;
    my ( @writeErrors, $writeConcernError, @upserted );

    # 'ok' false means GLE itself failed
    # usually we shouldn't check wnote or jnote, but the Bulk API QA test says we should
    # detect no journal or replication not enabled, so we check for special strings.
    # These strings were checked back to MongoDB 1.8.5.
    my $got_error =
        ( !$gle->{ok} ) ? $gle->{errmsg}
      : ( exists( $gle->{jnote} ) && $gle->{jnote} =~ NO_JOURNAL_RE )     ? $gle->{jnote}
      : ( exists( $gle->{wnote} ) && $gle->{wnote} =~ NO_REPLICATION_RE ) ? $gle->{wnote}
      :                                                                     undef;

    if ($got_error) {
        my $error_class =
          ( $got_error =~ /^not master/ )
          ? "MongoDB::NotMasterError"
          : "MongoDB::DatabaseError";
        $error_class->throw(
            message => $got_error,
            result  => MongoDB::CommandResult->new(
                result  => $gle,
                address => $link->address
            ),
        );
    }

    my $affected = 0;
    my $errmsg =
        defined $gle->{err}    ? $gle->{err}
      : defined $gle->{errmsg} ? $gle->{errmsg}
      :                          undef;
    my $wtimeout = $gle->{wtimeout};

    if ($wtimeout) {
        my $code = $gle->{code} || WRITE_CONCERN_ERROR;
        $writeConcernError = {
            errmsg  => $errmsg,
            errInfo => { wtimeout => $wtimeout },
            code    => $code
        };
    }

    if ( defined $errmsg && !$wtimeout ) {

        my $code = $gle->{code} || UNKNOWN_ERROR;
        # index is always 0 because ops are executed individually; later
        # merging of results will fix up the index values as usual
        my $error_doc = {
            errmsg => $errmsg,
            code   => $code,
            index  => 0,
            op     => $doc,
        };

        # convert boolean::true|false back to 1 or 0
        if ( $type eq 'update' ) {
            for my $k (qw/upsert multi/) {
                next unless exists $error_doc->{op}{$k};
                $error_doc->{op}{$k} = 0+ $error_doc->{op}{$k};
            };
        }

        $error_doc->{errInfo} = $gle->{errInfo} if exists $gle->{errInfo};

        push @writeErrors, $error_doc;
    }
    else {
        # GLE: n only returned for update/remove, so we infer it for insert
        $affected =
            $type eq 'insert' ? 1
          : defined $gle->{n} ? $gle->{n}
          :                     0;

        # For upserts, index is always 0 because ops are executed individually;
        # later merging of results will fix up the index values as usual.  For
        # 2.4 and earlier, 'upserted' has _id only if the _id is an OID.  Otherwise,
        # we have to pick it out of the update document or query document when we
        # see updateExisting is false but the number of docs affected is 1

        if ( exists $gle->{upserted} ) {
            push @upserted, { index => 0, _id => $gle->{upserted} };
        }
        elsif (exists $gle->{updatedExisting}
            && !$gle->{updatedExisting}
            && $gle->{n} == 1 )
        {
            my $id = exists $doc->{u}{_id} ? $doc->{u}{_id} : $doc->{q}{_id};
            push @upserted, { index => 0, _id => $id };
        }

    }

    my $result = MongoDB::WriteResult->_parse(
        op       => $type,
        op_count => 1,
        result   => {
            n                 => $affected,
            writeErrors       => \@writeErrors,
            writeConcernError => $writeConcernError,
            ( @upserted ? ( upserted => \@upserted ) : () ),
        },
    );

    return $result;
}

1;
