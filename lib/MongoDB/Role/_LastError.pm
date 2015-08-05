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

# XXX rename to _DBErrorSource;
package MongoDB::Role::_LastError;

# MongoDB interface for providing the last database error

use version;
our $VERSION = 'v0.999.999.5';

use Moo::Role;
use MongoDB::Error;
use namespace::clean;

# XXX should we add 'assert' to this API list?
requires qw/last_errmsg last_code last_wtimeout/;

my $ANY_DUP_KEY = [ DUPLICATE_KEY, DUPLICATE_KEY_UPDATE, DUPLICATE_KEY_CAPPED ];
my $ANY_NOT_MASTER = [ NOT_MASTER, NOT_MASTER_NO_SLAVE_OK, NOT_MASTER_OR_SECONDARY ];

# analyze last_errmsg and last_code and throw an appropriate
# error message.
sub _throw_database_error {
    my ( $self, $error_class ) = @_;
    $error_class ||= "MongoDB::DatabaseError";

    my $err  = $self->last_errmsg;
    my $code = $self->last_code;

    if ( grep { $code == $_ } @$ANY_NOT_MASTER || $err =~ /^(?:not master|node is recovering)/ ) {
        $error_class = "MongoDB::NotMasterError";
    }
    elsif ( grep { $code == $_ } @$ANY_DUP_KEY ) {
        $error_class = "MongoDB::DuplicateKeyError";
    }
    elsif ( $self->last_wtimeout ) {
        $error_class = "MongoDB::WriteConcernError";
    }

    $error_class->throw(
        result => $self,
        code   => $code || UNKNOWN_ERROR,
        ( length($err) ? ( message => $err ) : () ),
    );

}

1;
