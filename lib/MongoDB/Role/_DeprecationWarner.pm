#  Copyright 2016 - present MongoDB, Inc.
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
package MongoDB::Role::_DeprecationWarner;

# MongoDB interface for issuing deprecation warnings

use version;
our $VERSION = 'v1.999.0';

use Moo::Role;

use namespace::clean;

my %CALL_SITES;

sub _warn_deprecated {
    my ( $self, $old, $new ) = @_;

    return if $ENV{PERL_MONGO_NO_DEP_WARNINGS};

    my $msg = "# The '$old' method will be removed in a future major release.";

    # Arrayref is a list of replacement methods; string is just a message
    if ( ref $new eq 'ARRAY' ) {
        if ( @$new == 1 ) {
            $msg .= "\n# Use '$new->[0]' instead.";
        }
        elsif (@$new > 1) {
            my $last = pop @$new;
            my $list = join(", ", map { '$_' } @$new);
            $msg .= "\n# Use $list or '$last' instead.";
        }
    }
    elsif ( defined $new ) {
        $msg .= "\n# $new";
    }

    my ( $trace, $i ) = ( "", 0 );

    my ( $callsite_found, $pkg, $file, $line, $sub );

    # Accumulate the stack trace. Start at caller(1) to skip '_warn_deprecated'
    # in the stack trace
    while ( ++$i ) {
        # Use CORE::caller to get a real stack-trace, not one overridden by
        # CORE::GLOBAL::caller
        ( $pkg, $file, $line, $sub ) = CORE::caller($i);
        last unless defined $pkg;

        # We want to check the deprecated function's caller and shortcut if
        # we've already reported from that location.  As we walk up the
        # stack to build the trace, the first caller is usually the
        # call-site, but we ignore Sub::Uplevel and use the first
        # non-uplevel caller as the call-site.

        if ( !$callsite_found && $pkg ne 'Sub::Uplevel' ) {
            $callsite_found++;
            return if $CALL_SITES{ $pkg, $line, $file }++;
        }

        $trace .= "#    $sub called at $file line $line\n";
    }

    warn("#\n# *** DEPRECATION WARNING ***\n#\n$msg\n$trace");
}

1;
