#  Copyright 2014 - present MongoDB, Inc.
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

package MongoDB::Error;

# ABSTRACT: MongoDB Driver Error classes

# Portions adapted from Throwable.pm by Ricardo Signes

use version;

our $VERSION = 'v1.999.1';

use Moo;
use Carp;
use MongoDB::_Types qw(
    ErrorStr
);
use Scalar::Util ();
use Sub::Quote ();
use Safe::Isa;
use Exporter 5.57 qw/import/;
use namespace::clean -except => ['import'];

my $ERROR_CODES;

BEGIN {
    $ERROR_CODES = {
        BAD_VALUE                 => 2,
        HOST_UNREACHABLE          => 6,
        HOST_NOT_FOUND            => 7,
        UNKNOWN_ERROR             => 8,
        USER_NOT_FOUND            => 11,
        NAMESPACE_NOT_FOUND       => 26,
        INDEX_NOT_FOUND           => 27,
        CURSOR_NOT_FOUND          => 43,
        EXCEEDED_TIME_LIMIT       => 50,
        COMMAND_NOT_FOUND         => 59,
        WRITE_CONCERN_ERROR       => 64,
        NETWORK_TIMEOUT           => 89,
        SHUTDOWN_IN_PROGRESS      => 91,
        PRIMARY_STEPPED_DOWN      => 189,
        SOCKET_EXCEPTION          => 9001,
        NOT_MASTER                => 10107,
        DUPLICATE_KEY             => 11000,
        DUPLICATE_KEY_UPDATE      => 11001, # legacy before 2.6
        INTERRUPTED_AT_SHUTDOWN   => 11600,
        INTERRUPTED_DUE_TO_REPL_STATE_CHANGE => 11602,
        DUPLICATE_KEY_CAPPED      => 12582, # legacy before 2.6
        UNRECOGNIZED_COMMAND      => 13390, # mongos error before 2.4
        NOT_MASTER_NO_SLAVE_OK    => 13435,
        NOT_MASTER_OR_SECONDARY   => 13436,
        CANT_OPEN_DB_IN_READ_LOCK => 15927,
    };
}

use constant $ERROR_CODES;

# Export error codes for use by end-users; this is unusual for Moo, but
# probably sufficiently helpful to justify it
our @EXPORT = keys %$ERROR_CODES;

our %_HORRIBLE_HACK;

use overload (
    q{""} => sub {
        my $self = shift;
        return sprintf( "%s: %s", ref($self), $self->message );
    },
    fallback => 1
);

has message => (
    is      => 'ro',
    isa     => ErrorStr,
    default => 'unspecified error',
);

has 'previous_exception' => (
  is       => 'ro',
  default  => Sub::Quote::quote_sub(q<
    if (defined $MongoDB::Error::_HORRIBLE_HACK{ERROR}) {
      $MongoDB::Error::_HORRIBLE_HACK{ERROR}
    } elsif (defined $@ and (ref $@ or length $@)) {
      $@;
    } else {
      undef;
    }
  >),
);

sub throw {
  my ($inv) = shift;

  if (Scalar::Util::blessed($inv)) {
    Carp::confess "throw called on MongoDB::Error object with arguments" if @_;
    die $inv;
  }

  local $_HORRIBLE_HACK{ERROR} = $@;

  my $throwable = @_ == 1 ? $inv->new( message => $_[0] ) : $inv->new(@_);

  die $throwable;
}

# internal flag indicating if an operation should be retried when
# an error occurs.
sub _is_resumable { 1 }

# internal flag for if this error type specifically can be retried regardless
# of other state. See _is_retryable which contains the full retryable error
# logic.
sub __is_retryable_error { 0 }

sub _check_is_retryable_code {
    my $code = $_[-1];

    my @retryable_codes = (
        MongoDB::Error::HOST_NOT_FOUND(),
        MongoDB::Error::HOST_UNREACHABLE(),
        MongoDB::Error::NETWORK_TIMEOUT(),
        MongoDB::Error::SHUTDOWN_IN_PROGRESS(),
        MongoDB::Error::PRIMARY_STEPPED_DOWN(),
        MongoDB::Error::SOCKET_EXCEPTION(),
        MongoDB::Error::NOT_MASTER(),
        MongoDB::Error::INTERRUPTED_AT_SHUTDOWN(),
        MongoDB::Error::INTERRUPTED_DUE_TO_REPL_STATE_CHANGE(),
        MongoDB::Error::NOT_MASTER_NO_SLAVE_OK(),
        MongoDB::Error::NOT_MASTER_OR_SECONDARY(),
    );

    return 1 if grep { $code == $_ } @retryable_codes;
    return 0;
}

sub _check_is_retryable_message {
  my $message = $_[-1];

  return 1 if $message =~ /(not master|node is recovering)/i;
  return 0;
}

# indicates if this error can be retried under retryable writes
sub _is_retryable {
    my $self = shift;

    if ( $self->$_can( 'result' ) ) {
        return 1 if _check_is_retryable_code( $self->result->last_code );
    }

    if ( $self->$_can( 'code' ) ) {
        return 1 if _check_is_retryable_code( $self->code );
    }

    return 1 if _check_is_retryable_message( $self->message );

    if ( $self->$_isa( 'MongoDB::WriteConcernError' ) ) {
      return 1 if _check_is_retryable_code( $self->result->output->{writeConcernError}{code} );
      return 1 if _check_is_retryable_message( $self->result->output->{writeConcernError}{message} );
    }

    # Defaults to 0 unless its a network exception
    return $self->__is_retryable_error;
}

#--------------------------------------------------------------------------#
# Subclasses with attributes included inline below
#--------------------------------------------------------------------------#

package MongoDB::DatabaseError;
use Moo;
use MongoDB::_Types qw(
    Numish
);
use namespace::clean;

extends("MongoDB::Error");

has result => (
    is       => 'ro',
    does     => 'MongoDB::Role::_DatabaseErrorThrower',
    required => 1,
);

has code => (
    is      => 'ro',
    isa     => Numish,
    builder => '_build_code',
);

sub _build_code { return MongoDB::Error::UNKNOWN_ERROR() }

sub _is_resumable { 0 }

package MongoDB::DocumentError;

use Moo;
use Types::Standard qw(Any);
use namespace::clean;

extends("MongoDB::Error");

has document => (
    is       => 'ro',
    isa      => Any,
    required => 1,
);

package MongoDB::UsageError;

use Moo;
use Types::Standard qw(Str);
use namespace::clean -except => 'meta';

extends("MongoDB::Error");

use overload (
    q{""} => sub {
        my $self = shift;
        return sprintf( "%s: %s%s", ref($self), $self->message, $self->trace );
    },
    fallback => 1
);

has trace => (
    is  => 'ro',
    isa => Str,
);

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my $args  = $class->SUPER::BUILDARGS(@_);
    # start stack trace above where throw() is called (or
    # at the top of the stack), so it works like confess
    my $i = 0;
    while ( my @caller = caller($i) ) {
        $i++;
        last if $caller[0] eq "MongoDB::Error";
    }
    local $Carp::CarpLevel = caller( $i + 1 ) ? $i + 1 : $i;
    $args->{trace} = Carp::longmess('');
    return $args;
};

# Connection errors
package MongoDB::ConnectionError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

sub _is_resumable { 1 }
sub __is_retryable_error { 1 }

package MongoDB::HandshakeError;
use Moo;
use namespace::clean;
extends 'MongoDB::ConnectionError';

package MongoDB::NetworkError;
use Moo;
use namespace::clean;
extends 'MongoDB::ConnectionError';

# Timeout errors
package MongoDB::TimeoutError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

sub __is_retryable_error { 1 }

package MongoDB::ExecutionTimeout;
use Moo;
use namespace::clean;
extends 'MongoDB::TimeoutError';

package MongoDB::NetworkTimeout;
use Moo;
use namespace::clean;
extends 'MongoDB::TimeoutError';

# Database errors
package MongoDB::DuplicateKeyError;
use Moo;
use namespace::clean;
extends 'MongoDB::DatabaseError';
sub _build_code { return MongoDB::Error::DUPLICATE_KEY() }

package MongoDB::NotMasterError;
use Moo;
use namespace::clean;
extends 'MongoDB::DatabaseError';
sub _build_code { return MongoDB::Error::NOT_MASTER() }
sub _is_resumable { 1 }

package MongoDB::WriteError;
use Moo;
use namespace::clean;
extends 'MongoDB::DatabaseError';

package MongoDB::WriteConcernError;
use Moo;
use namespace::clean;
extends 'MongoDB::DatabaseError';
sub _build_code { return MongoDB::Error::WRITE_CONCERN_ERROR() }

# Other errors
package MongoDB::AuthError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::ConfigurationError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::CursorNotFoundError;
use Moo;
use namespace::clean;
extends 'MongoDB::DatabaseError';
sub _build_code { return MongoDB::Error::CURSOR_NOT_FOUND() }
sub _is_resumable { 1 }

package MongoDB::DecodingError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::GridFSError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::InternalError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::ProtocolError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::SelectionError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

package MongoDB::InvalidOperationError;
use Moo;
use namespace::clean;
extends 'MongoDB::Error';

#--------------------------------------------------------------------------#
# Private error classes
#--------------------------------------------------------------------------#
package MongoDB::_CommandSizeError;
use Moo;
use MongoDB::_Types qw(
    Intish
);
use namespace::clean;

extends("MongoDB::Error");

has size => (
    is       => 'ro',
    isa      => Intish,
    required => 1,
);

1;

__END__

=head1 SYNOPSIS

    use MongoDB::Error;
    MongoDB::Error->throw("a generic error");
    MongoDB::DatabaseError->throw(
        message => $string,
        result => $hashref,
    );

=head1 DESCRIPTION

This class defines a hierarchy of exception objects.

=head1 USAGE

Unless otherwise explicitly documented, all driver methods throw exceptions if
an error occurs.

To catch and handle errors, the L<Try::Tiny> and L<Safe::Isa> modules
are recommended:

    use Try::Tiny;
    use Safe::Isa; # provides $_isa

    try {
        $coll->insert( $doc )
    }
    catch {
        if ( $_->$_isa("MongoDB::DuplicateKeyError" ) ) {
            ...
        }
        else {
            ...
        }
    };

To retry failures automatically, consider using L<Try::Tiny::Retry>.

=head1 EXCEPTION HIERARCHY

    MongoDB::Error
        |
        |->MongoDB::AuthError
        |
        |->MongoDB::ConnectionError
        |   |
        |   |->MongoDB::HandshakeError
        |   |
        |   |->MongoDB::NetworkError
        |
        |->MongoDB::ConfigurationError
        |
        |->MongoDB::DatabaseError
        |   |
        |   |->MongoDB::CursorNotFoundError
        |   |
        |   |->MongoDB::DuplicateKeyError
        |   |
        |   |->MongoDB::NotMasterError
        |   |
        |   |->MongoDB::WriteError
        |   |
        |   |->MongoDB::WriteConcernError
        |
        |->MongoDB::DecodingError
        |
        |->MongoDB::DocumentError
        |
        |->MongoDB::GridFSError
        |
        |->MongoDB::InternalError
        |
        |->MongoDB::InvalidOperationError
        |
        |->MongoDB::ProtocolError
        |
        |->MongoDB::SelectionError
        |
        |->MongoDB::TimeoutError
        |   |
        |   |->MongoDB::ExecutionTimeout
        |   |
        |   |->MongoDB::NetworkTimeout
        |
        |->MongoDB::UsageError

All classes inherit from C<MongoDB::Error>.

All error classes have the attribute:

=for :list
* message — a text representation of the error

=head2 MongoDB::AuthError

This error indicates a problem with authentication, either in the underlying
mechanism or a problem authenticating with the server.

=head2 MongoDB::ConnectionError

Errors related to network connections.

=head3 MongoDB::HandshakeError

This error is thrown when a connection has been made, but SSL or authentication
handshakes fail.

=head3 MongoDB::NetworkError

This error is thrown when a socket error occurs, when the wrong number of bytes
are read, or other wire-related errors occur.

=head2 MongoDB::ConfigurationError

This error is thrown when there is a configuration error between the MongoDB
deployment and the configuration of the client, such as when trying to use
explicit sessions on a MongoDB < 3.6

=head2 MongoDB::CursorNotFoundError

This error indicates that a cursor timed out on a server.

=head2 MongoDB::DatabaseError

Errors related to database operations.  Specifically, when an error of this type
occurs, the driver has received an error condition from the server.

Attributes include:

=for :list
* result — response from a database command; this must impliement the
  C<last_errmsg> method
* code — numeric error code; see L</ERROR CODES>; if no code was provided
  by the database, the C<UNKNOWN_ERROR> code will be substituted instead

=head3 MongoDB::DuplicateKeyError

This error indicates that a write attempted to create a document with a
duplicate key in a collection with a unique index.  The C<result> attribute is
a result object.

=head3 MongoDB::NotMasterError

This error indicates that a write or other state-modifying operation was
attempted on a server that was not a primary.  The C<result> attribute is
a L<MongoDB::CommandResult> object.

=head3 MongoDB::WriteError

Errors indicating failure of a write command.  The C<result> attribute is
a result object.

=head3 MongoDB::WriteConcernError

Errors indicating failure of a write concern.  The C<result> attribute is a
result object.

=head2 MongoDB::DecodingError

This error indicates a problem during BSON decoding; it wraps
the error provided by the underlying BSON encoder.  Note: Encoding errors
will be thrown as a L</MongoDB::DocumentError>.

=head2 MongoDB::DocumentError

This error indicates a problem with a document to be inserted or replaced into
the database, or used as an update document.

Attributes include:

=for :list
* document — the document that caused the error

=head2 MongoDB::GridFSError

Errors related to GridFS operations, such a corrupted file.

=head2 MongoDB::InternalError

Errors that indicate problems in the driver itself, typically when something
unexpected is detected.  These should be reported as potential bugs.

=head2 MongoDB::ProtocolError

Errors related to the MongoDB wire protocol, typically problems parsing a
database response packet.

=head2 MongoDB::SelectionError

When server selection fails for a given operation, this is thrown. For example,
attempting a write when no primary is available or reading with a specific mode
and tag set and no servers match.

=head2 MongoDB::TimeoutError

These errors indicate a user-specified timeout has been exceeded.

=head3 MongoDB::ExecutionTimeout

This error is thrown when a query or command fails because C<max_time_ms> has
been reached.  The C<result> attribute is a L<MongoDB::CommandResult> object.

=head3 MongoDB::NetworkTimeout

This error is thrown when a network operation exceeds a timeout, typically
C<connect_timeout_ms> or C<socket_timeout_ms>.

=head2 MongoDB::UsageError

Indicates invalid arguments or configuration options.  Not all usage errors
will throw this — only ones originating directly from the MongoDB::* library
files.  Some type and usage errors will originate from the L<Type::Tiny>
library if the objects are used incorrectly.

=head1 ERROR CODES

The following error code constants are automatically exported by this module.

        BAD_VALUE                 => 2,
        UNKNOWN_ERROR             => 8,
        NAMESPACE_NOT_FOUND       => 26,
        EXCEEDED_TIME_LIMIT       => 50,
        COMMAND_NOT_FOUND         => 59,
        WRITE_CONCERN_ERROR       => 64,
        NOT_MASTER                => 10107,
        DUPLICATE_KEY             => 11000,
        DUPLICATE_KEY_UPDATE      => 11001, # legacy before 2.6
        DUPLICATE_KEY_CAPPED      => 12582, # legacy before 2.6
        UNRECOGNIZED_COMMAND      => 13390, # mongos error before 2.4
        NOT_MASTER_NO_SLAVE_OK    => 13435,
        NOT_MASTER_OR_SECONDARY   => 13436,
        CANT_OPEN_DB_IN_READ_LOCK => 15927,

This is a very, very small subset of error codes possible from the server,
but covers some of the more common ones seen by drivers.

B<Note>:

=for :list
* Only C<MongoDB::DatabaseError> objects have a C<code> attribute.
* The database uses multiple write concern error codes.  The driver maps
  them all to WRITE_CONCERN_ERROR for consistency and convenience.

=cut

# vim: ts=4 sts=4 sw=4 et:
