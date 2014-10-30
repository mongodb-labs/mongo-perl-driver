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

package MongoDB::Role::_HasReadPreference;

# MongoDB role for getting/setting a read preference

use version;
our $VERSION = 'v0.704.4.1';

use Moose::Role;
use MongoDB::ReadPreference;
use MongoDB::_Types;
use namespace::clean -except => 'meta';

has _read_preference => (
    is        => 'ro',
    isa       => 'ReadPreference',
    init_arg  => 'read_preference',
    writer    => '_set_read_preference',
    predicate => '_has_read_preference',
    coerce    => 1,
);

sub read_preference {
    my $self = shift;

    # XXX eventually, do this via type coercion?
    my $type = ref $_[0];
    if ( $type eq 'MongoDB::ReadPreference' ) {
        $self->_set_read_preference( $_[0] );
    }
    else {
        my $mode     = shift || 'primary';
        my $tag_sets = shift;
        my $rp       = MongoDB::ReadPreference->new(
            mode => $mode,
            ( $tag_sets ? ( tag_sets => $tag_sets ) : () )
        );
        $self->_set_read_preference($rp);
    }

    return $self;
}

1;
