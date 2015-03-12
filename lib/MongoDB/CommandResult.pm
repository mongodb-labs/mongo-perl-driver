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

package MongoDB::CommandResult;

# ABSTRACT: MongoDB generic command result document

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use Moose;
use MongoDB::Error;
use MongoDB::_Types -types;
use Types::Standard -types;
use namespace::clean -except => 'meta';

with 'MongoDB::Role::_LastError';

=attr result

Hash reference with the result of a database command

=cut

has result => (
    is       => 'ro',
    isa      => HashRef,
    required => 1,
);

=attr address

Address ("host:port") of server that ran the command

=cut

has address => (
    is       => 'ro',
    isa      => HostAddress,
    required => 1,
);

=method last_code

Error code (if any) or 0 if there was no error.

=cut

sub last_code {
    my ($self) = @_;
    my $result = $self->result;
    if ( $result->{code} ) {
        return $result->{code};
    }
    elsif ( $result->{lastErrorObject} ) {
        return $result->{lastErrorObject}{code} || 0;
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
    for my $err_key (qw/$err err errmsg/) {
        return $self->result->{$err_key} if exists $self->result->{$err_key};
    }
    return "";
}

=method last_wtimeout

True if a write concern timed out or false otherwise.

=cut

sub last_wtimeout {
    my ($self) = @_;
    return !!$self->result->{wtimeout};
}

=method assert

Throws an exception if the command failed.

=cut

sub assert {
    my ($self, $default_class) = @_;

    $self->_throw_database_error( $default_class )
        if ! $self->result->{ok};

    return 1;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 DESCRIPTION

This class encapsulates the results from a database command.  Currently, it is only
available from the C<result> attribute of C<MongoDB::DatabaseError>.

=cut
