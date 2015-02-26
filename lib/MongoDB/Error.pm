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

use 5.008;
use strict;
use warnings;

package MongoDB::Error;
# ABSTRACT: MongoDB Driver Error classes

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;
use Moose::Meta::Class ();
use MongoDB::_Types -types;
use Types::Standard -types;
use Exporter 5.57 qw/import/;
use namespace::clean -except => [ 'meta', 'import' ];

my $ERROR_CODES;

BEGIN {
    $ERROR_CODES = {
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
    };
}

use constant $ERROR_CODES;

# Export error codes for use by end-users; this is unusual for Moose, but
# probably sufficiently helpful to justify it
our @EXPORT = keys %$ERROR_CODES;

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

=method throw

    MongoDB::Error->throw( "message" );
    MongoDB::Error->throw( message => "message" );
    MongoDB::Error->throw( $error_object );

=cut

with 'Throwable';

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    if ( @_ == 1 && !ref $_[0] ) {
        return $class->$orig( message => $_[0] );
    }
    return $class->$orig(@_);
};

#--------------------------------------------------------------------------#
# Subclasses with attributes included inline below
#--------------------------------------------------------------------------#

package MongoDB::DatabaseError;
use Moose;
use Types::Standard -types;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

# XXX should rename to 'details' or 'error' or something less confusing than
# the word 'result'

has result => (
    is       => 'ro',
    does     => 'MongoDB::Role::_LastError',
    required => 1,
);

has code => (
    is      => 'ro',
    isa     => Num,
    builder => '_build_code',
);

sub _build_code { return MongoDB::Error::UNKNOWN_ERROR() }

package MongoDB::DocumentError;
use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

has document => (
    is       => 'ro',
    isa      => HashRef|IxHash,
    required => 1,
);

#--------------------------------------------------------------------------#
# Empty subclasses generated programatically; this keeps packages visible
# to metadata inspectors, but is shorter than Moose/namespace::clean/extends
#--------------------------------------------------------------------------#

# Connection errors
package MongoDB::ConnectionError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

package MongoDB::HandshakeError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::ConnectionError'] );

package MongoDB::NetworkError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::ConnectionError'] );

# Timeout errors
package MongoDB::TimeoutError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

package MongoDB::ExecutionTimeout;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::TimeoutError'] );

package MongoDB::NetworkTimeout;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::TimeoutError'] );

# Database errors
package MongoDB::DuplicateKeyError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

sub _build_code { return MongoDB::Error::DUPLICATE_KEY() }

package MongoDB::NotMasterError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

sub _build_code { return MongoDB::Error::NOT_MASTER() }

package MongoDB::WriteError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

package MongoDB::WriteConcernError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

sub _build_code { return MongoDB::Error::WRITE_CONCERN_ERROR() }

# Other errors
package MongoDB::CursorNotFoundError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

package MongoDB::ProtocolError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

package MongoDB::SelectionError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

package MongoDB::InternalError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

#--------------------------------------------------------------------------#
# Private error classes
#--------------------------------------------------------------------------#

package MongoDB::_CommandSizeError;
use Moose;
use Types::Standard -types;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

has size => (
    is       => 'ro',
    isa      => Int,
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

This class defines a heirarchy of exception objects.

=head1 EXCEPTION HIERARCHY

    MongoDB::Error
        |
        |->MongoDB::ConnectionError
        |   |
        |   |->MongoDB::HandshakeError
        |   |
        |   |->MongoDB::NetworkError
        |
        |->MongoDB::DatabaseError
        |   |
        |   |->MongoDB::DuplicateKeyError
        |   |
        |   |->MongoDB::NotMasterError
        |   |
        |   |->MongoDB::WriteError
        |   |
        |   |->MongoDB::WriteConcernError
        |
        |->MongoDB::TimeoutError
        |   |
        |   |->MongoDB::ExecutionTimeout
        |   |
        |   |->MongoDB::NetworkTimeout
        |
        |->MongoDB::CursorNotFoundError
        |
        |->MongoDB::DocumentError
        |
        |->MongoDB::ProtocolError
        |
        |->MongoDB::SelectionError
        |
        |->MongoDB::InternalError


All classes inherit from C<MongoDB::Error>.

All error classes have the attribute:

=for :list
* message — a text representation of the error

=head2 MongoDB::ConnectionError

Errors related to network connections.

=head3 MongoDB::HandshakeError

This error is thrown when a connection has been made, but SSL or authentication
handshakes fail.

=head3 MongoDB::NetworkError

This error is thrown when a socket error occurs, when the wrong number of bytes
are read, or other wire-related errors occur.

=head2 MongoDB::DatabaseError

Errors related to database operations.  Specifically, when an error of this type
occurs, the driver has received an error condition from the server.

Attributes include:

=for :list
* result — response from a database command; this must impliement the
  C<last_errmsg> method

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

=head2 MongoDB::TimeoutError

These errors indicate a user-specified timeout has been exceeded.

=head3 MongoDB::ExecutionTimeout

This error is thrown when a query or command fails because C<max_time_ms> has
been reached.  The C<result> attribute is a L<MongoDB::CommandResult> object.

=head3 MongoDB::NetworkTimeout

This error is thrown when a network operation exceeds a timeout, typically
C<connect_timeout_ms> or C<socket_timeout_ms>.

=head2 MongoDB::CursorNotFoundError

This error indicates that a cursor timed out on a server.

=head2 MongoDB::DocumentError

This error indicates a problem with a document to be inserted or replaced into
the database.

Attributes include:

=for :list
* document — the document that caused the error

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

=cut

# vim: ts=4 sts=4 sw=4 et:
