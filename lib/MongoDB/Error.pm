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
our $VERSION = 'v0.999.999.5';

use Moose;
use Carp;
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

__PACKAGE__->meta->make_immutable;

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

__PACKAGE__->meta->make_immutable;

package MongoDB::DocumentError;
use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

has document => (
    is       => 'ro',
    isa      => Any,
    required => 1,
);

__PACKAGE__->meta->make_immutable;

package MongoDB::UsageError;
use Moose;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

use overload (
    q{""} => sub {
        my $self = shift;
        return
          sprintf( "%s: %s%s", ref($self), $self->message, $self->trace );
    },
    fallback => 1
);

has trace => (
    is       => 'ro',
    isa      => Str,
);

around BUILDARGS => sub {
    my $orig  = shift;
    my $class = shift;
    my $args = $class->SUPER::BUILDARGS(@_);

    # start stack trace above where throw() is called (or
    # at the top of the stack), so it works like confess
    my $i = 0;
    while ( my @caller = caller($i) ) {
        $i++;
        last if $caller[0] eq "Throwable";
    }
    local $Carp::CarpLevel = caller($i + 1)? $i + 1 : $i;
    $args->{trace} = Carp::longmess('');
    return $args;
};

__PACKAGE__->meta->make_immutable;

#--------------------------------------------------------------------------#
# Empty subclasses generated programatically; this keeps packages visible
# to metadata inspectors, but is shorter than Moose/namespace::clean/extends
#--------------------------------------------------------------------------#

# Connection errors

package MongoDB::ConnectionError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::HandshakeError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::ConnectionError'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::NetworkError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::ConnectionError'] );

__PACKAGE__->meta->make_immutable;

# Timeout errors

package MongoDB::TimeoutError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::ExecutionTimeout;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::TimeoutError'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::NetworkTimeout;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::TimeoutError'] );

__PACKAGE__->meta->make_immutable;

# Database errors

package MongoDB::DuplicateKeyError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

sub _build_code { return MongoDB::Error::DUPLICATE_KEY() }

__PACKAGE__->meta->make_immutable;

package MongoDB::NotMasterError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

sub _build_code { return MongoDB::Error::NOT_MASTER() }

__PACKAGE__->meta->make_immutable;

package MongoDB::WriteError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::WriteConcernError;
Moose::Meta::Class->create( __PACKAGE__,
    superclasses => ['MongoDB::DatabaseError'] );

sub _build_code { return MongoDB::Error::WRITE_CONCERN_ERROR() }

__PACKAGE__->meta->make_immutable;

# Other errors

package MongoDB::AuthError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::CursorNotFoundError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::DecodingError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::GridFSError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::InternalError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::ProtocolError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

package MongoDB::SelectionError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => ['MongoDB::Error'] );

__PACKAGE__->meta->make_immutable;

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

__PACKAGE__->meta->make_immutable;

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

=head1 USAGE

Unless otherwise explictly documented, all driver methods throw exceptions if
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
        |->MongoDB::CursorNotFoundError
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
        |->MongoDB::DecodingError
        |
        |->MongoDB::DocumentError
        |
        |->MongoDB::GridFSError
        |
        |->MongoDB::InternalError
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
files.  Some type and usage errors will originate from the L<Moose> object
system if the objects are used incorrectly.

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

B<Note>: only C<MongoDB::DatabaseError> objects have a C<code> attribute.

=cut

# vim: ts=4 sts=4 sw=4 et:
