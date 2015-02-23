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

package MongoDB::Role::_WriteResult;

# MongoDB interface for common write result attributes and methods

use version;
our $VERSION = 'v0.999.998.3'; # TRIAL

use MongoDB::Error;
use MongoDB::_Types;
use Moose::Role;
use namespace::clean -except => 'meta';

has acknowledged => (
    is      => 'ro',
    isa     => 'Bool',
    default => 1,
);

has [qw/write_errors write_concern_errors/] => (
    is      => 'ro',
    isa     => 'ArrayOfHashRef',
    coerce  => 1,
    default => sub { [] },
);

with 'MongoDB::Role::_LastError';

sub assert {
    my ($self) = @_;
    $self->assert_no_write_error;
    $self->assert_no_write_concern_error;
    return 1;
}

sub assert_no_write_error {
    my ($self) = @_;

    $self->_throw_database_error("MongoDB::WriteError")
      if $self->count_write_errors;
    return 1;
}

sub assert_no_write_concern_error {
    my ($self) = @_;
    if ( $self->count_write_concern_errors ) {
        MongoDB::WriteConcernError->throw(
            message => $self->last_errmsg,
            result  => $self,
            code    => WRITE_CONCERN_ERROR,
        );
    }
    return 1;
}

sub count_write_errors {
    my ($self) = @_;
    return scalar @{ $self->write_errors };
}

sub count_write_concern_errors {
    my ($self) = @_;
    return scalar @{ $self->write_concern_errors };
}

sub last_errmsg {
    my ($self) = @_;
    if ( $self->count_write_errors ) {
        return $self->write_errors->[-1]{errmsg};
    }
    elsif ( $self->count_write_concern_errors ) {
        return $self->write_concern_errors->[-1]{errmsg};
    }
    else {
        return "";
    }
}

sub last_code {
    my ($self) = @_;
    if ( $self->count_write_errors ) {
        return $self->write_errors->[-1]{code} || UNKNOWN_ERROR;
    }
    elsif ( $self->count_write_concern_errors ) {
        return $self->write_concern_errors->[-1]{code} || UNKNOWN_ERROR;
    }
    else {
        return 0;
    }
}

sub last_wtimeout {
    my ($self) = @_;
    # if we have actual write errors, we don't want to report a
    # write concern error
    return !!( $self->count_write_concern_errors && !$self->count_write_errors );
}

1;
