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
package MongoDB::CommandResult;

# ABSTRACT: MongoDB generic command result document

use version;
our $VERSION = 'v1.999.0';

use Moo;
use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    HostAddress
);
use Types::Standard qw(
    HashRef
);
use namespace::clean;

with $_ for qw(
  MongoDB::Role::_PrivateConstructor
  MongoDB::Role::_DatabaseErrorThrower
  MongoDB::Role::_DeprecationWarner
);

=attr output

Hash reference with the output document of a database command

=cut

has output => (
    is       => 'ro',
    required => 1,
    isa => HashRef,
);

=attr address

Address ("host:port") of server that ran the command

=cut

has address => (
    is       => 'ro',
    required => 1,
    isa => HostAddress,
);

=method last_code

Error code (if any) or 0 if there was no error.

=cut

sub last_code {
    my ($self) = @_;
    my $output = $self->output;
    if ( $output->{code} ) {
        return $output->{code};
    }
    elsif ( $output->{lastErrorObject} ) {
        return $output->{lastErrorObject}{code} || 0;
    }
    elsif ( $output->{writeConcernError} ) {
        return $output->{writeConcernError}{code} || 0;
    }
    else {
        return 0;
    }
}

=method last_errmsg

Error string (if any) or the empty string if there was no error.

=cut

sub last_errmsg {
    my ($self) = @_;
    my $output = $self->output;
    for my $err_key (qw/$err err errmsg/) {
        return $output->{$err_key} if exists $output->{$err_key};
    }
    if ( exists $output->{writeConcernError} ) {
        return $output->{writeConcernError}{errmsg}
    }
    return "";
}

=method last_wtimeout

True if a write concern error or timeout occurred or false otherwise.

=cut

sub last_wtimeout {
    my ($self) = @_;
    return !!( exists $self->output->{wtimeout}
        || exists $self->output->{writeConcernError} );
}

=method assert

Throws an exception if the command failed.

=cut

sub assert {
    my ($self, $default_class) = @_;

    $self->_throw_database_error( $default_class )
        if ! $self->output->{ok};

    return 1;
}

=method assert_no_write_concern_error

Throws an exception if a write concern error occurred

=cut

sub assert_no_write_concern_error {
    my ($self) = @_;

    $self->_throw_database_error( "MongoDB::WriteConcernError" )
        if $self->last_wtimeout;

    return 1;
}

# deprecated
sub result {
    my $self = shift;

    $self->_warn_deprecated( 'result' => [qw/output/] );

    return $self->output;
}

1;

__END__

=for Pod::Coverage
result

=head1 DESCRIPTION

This class encapsulates the results from a database command.  Currently, it is
only available from the C<result> attribute of C<MongoDB::DatabaseError>.

=head1 DEPRECATIONS

The methods still exist, but are no longer documented.  In a future version
they will warn when used, then will eventually be removed.

=for :list
* result

=cut
