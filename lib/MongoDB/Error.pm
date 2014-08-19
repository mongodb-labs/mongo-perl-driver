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
our $VERSION = 'v0.704.5.1';

use Moose;
use Moose::Meta::Class ();
use MongoDB::_Types;
use Exporter 5.57 qw/import/;
use namespace::clean -except => [ 'meta', 'import' ];

my $ERROR_CODES;

BEGIN {
    $ERROR_CODES = {
        BAD_VALUE                   => 2,
        UNKNOWN_ERROR               => 8,
        WRITE_CONCERN_ERROR         => 64,
        CANT_OPEN_DB_IN_READ_LOCK   => 15927,
    };
}

use constant $ERROR_CODES;

# Export error codes for use by end-users; this is unusual for Moose, but
# probably sufficiently helpful to justify it
our @EXPORT = keys %$ERROR_CODES;

use overload
  q{""}    => sub { shift->message },
  fallback => 1;

has message => (
    is      => 'ro',
    isa     => 'Str',
    default => 'unspecified error',
);

=method throw

    MongoDB::Error->throw("message");
    MongoDB::Error->throw(
        msg => "message",
        result => $data,
    );
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
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

has result => (
    is       => 'ro',
    does     => 'MongoDB::Role::_LastError',
    required => 1,
);

package MongoDB::DocumentSizeError;
use Moose;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

has document => (
    is       => 'ro',
    isa      => 'HashRef|IxHash',
    required => 1,
);

#--------------------------------------------------------------------------#
# Empty subclasses generated programatically; this keeps packages visible
# to metadata inspectors, but is shorter than Moose/namespace::clean/extends
#--------------------------------------------------------------------------#

package MongoDB::ConnectionError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => [ 'MongoDB::Error' ] );

package MongoDB::WriteError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => [ 'MongoDB::DatabaseError' ] );

package MongoDB::WriteConcernError;
Moose::Meta::Class->create( __PACKAGE__, superclasses => [ 'MongoDB::DatabaseError' ] );

#--------------------------------------------------------------------------#
# Internal error classes
#--------------------------------------------------------------------------#

package MongoDB::_CommandSizeError;
use Moose;
use namespace::clean -except => 'meta';
extends("MongoDB::Error");

has size => (
    is       => 'ro',
    isa      => 'Int',
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

All classes inherit from C<MongoDB::Error>.

All error classes have the attribute:

=for :list
* message — a text representation of the error

=cut

=head2 MongoDB::ConnectionError

Errors related to network connections.

=head2 MongoDB::DatabaseError

Errors related to database operations.

Attributes include:

=for :list
* result — response from a database command; this must impliement the
  C<last_errmsg> method

=head3 MongoDB::WriteError

Errors indicating failure of a write command.  The C<result> attribute is
a L<MongoDB::WriteResult> object.

=head3 MongoDB::WriteConcernError

Errors indicating failure of a write concern.  The C<result> attribute is a
L<MongoDB::WriteResult> object.

=head2 MongoDB::DocumentSizeError

Errors from documents exceeding the maximum allowable size.

Attributes include:

=for :list
* document — the document that caused the error

=cut

# vim: ts=4 sts=4 sw=4 et:
