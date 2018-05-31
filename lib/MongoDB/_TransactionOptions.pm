#  Copyright 2018 - present MongoDB, Inc.
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
package MongoDB::_TransactionOptions;

# MongoDB options for transactions

use version;
our $VERSION = 'v1.999.0';

use MongoDB::Error;

use Moo;
use MongoDB::ReadConcern;
use MongoDB::WriteConcern;
use MongoDB::ReadPreference;
use MongoDB::_Types qw(
    MongoDBClient
    WriteConcern
    ReadConcern
    ReadPreference
);
use Types::Standard qw(
    HashRef
    Any
);
use namespace::clean -except => 'meta';

# Options provided during start transaction
has options => (
    is => 'ro',
    required => 1,
    isa => HashRef,
);

# Options provided during start session
has default_options => (
    is => 'ro',
    required => 1,
    isa => HashRef,
);

# needed for defaults
has client => (
    is => 'ro',
    required => 1,
    isa => MongoDBClient,
);

has write_concern => (
    # must error on start_transaction, so is built immediately
    is => 'ro',
    isa => WriteConcern,
    init_arg => undef,
    builder => '_build_write_concern',
);

sub _build_write_concern {
    my $self = shift;

    my $options = $self->options->{writeConcern};
    $options ||= $self->default_options->{writeConcern};

    my $write_concern;
    $write_concern = MongoDB::WriteConcern->new( $options ) if defined $options;
    $write_concern ||= $self->client->write_concern;

    unless ( $write_concern->is_acknowledged ) {
        MongoDB::ConfigurationError->throw(
            'transactions do not support unacknowledged write concerns' );
    }

    return $write_concern;
}

has read_concern => (
    is => 'lazy',
    isa => ReadConcern,
    init_arg => undef,
    builder => '_build_read_concern',
);

# Read concern errors are returned by the database, so no need to check for
# errors
sub _build_read_concern {
    my $self = shift;

    my $options = $self->options->{readConcern};
    $options ||= $self->default_options->{readConcern};

    return MongoDB::ReadConcern->new( $options ) if defined $options;
    return $self->client->read_concern;
}

has read_preference => (
    is => 'lazy',
    isa => ReadPreference,
    init_arg => undef,
    builder => '_build_read_preference',
);

# Read preferences must be primary at present, so check after building it
sub _build_read_preference {
    my $self = shift;

    my $options = $self->options->{readPreference};
    $options ||= $self->default_options->{readPreference};

    my $read_pref;
    $read_pref = MongoDB::ReadPreference->new( $options ) if defined $options;
    $read_pref ||= $self->client->read_preference;

    if ( $read_pref->mode ne 'primary' ) {
        MongoDB::ConfigurationError->throw(
            "read preference in a transaction must be primary" );
    }

    return $read_pref;
}

1;
