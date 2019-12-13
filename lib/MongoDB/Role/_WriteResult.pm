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
package MongoDB::Role::_WriteResult;

# MongoDB interface for common write result attributes and methods

use version;
our $VERSION = 'v2.2.2';

use Moo::Role;

use MongoDB::Error;
use MongoDB::_Constants;
use MongoDB::_Types qw(
    ArrayOfHashRef
);

use namespace::clean;

has [qw/write_errors write_concern_errors/] => (
    is       => 'ro',
    required => 1,
    isa      => ArrayOfHashRef,
);

with 'MongoDB::Role::_DatabaseErrorThrower';

sub acknowledged { 1 }; # override to 0 for MongoDB::UnacknowledgedResult

# inline assert_no_write_error and assert_no_write_concern rather
# than having to make to additional method calls
sub assert {
    my ($self) = @_;

    $self->_throw_database_error("MongoDB::WriteError")
      if scalar @{ $self->write_errors };

    MongoDB::WriteConcernError->throw(
        message => $self->last_errmsg,
        result  => $self,
        code    => WRITE_CONCERN_ERROR,
    ) if scalar @{ $self->write_concern_errors };

    return $self;
}

sub assert_no_write_error {
    my ($self) = @_;

    $self->_throw_database_error("MongoDB::WriteError")
      if scalar @{ $self->write_errors };

    return $self;
}

sub assert_no_write_concern_error {
    my ($self) = @_;

    MongoDB::WriteConcernError->throw(
        message => $self->last_errmsg,
        result  => $self,
        code    => WRITE_CONCERN_ERROR,
    ) if scalar @{ $self->write_concern_errors };

    return $self;
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

sub last_error_labels {
    my ( $self ) = @_;
    if ( $self->count_write_errors ) {
        return $self->write_errors->[-1]{errorLabels} || [];
    }
    elsif ( $self->count_write_concern_errors ) {
        return $self->write_errors->[-1]{errorLabels} || [];
    }
    return [];
}

1;
