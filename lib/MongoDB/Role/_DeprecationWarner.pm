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
our $VERSION = 'v2.2.2';

use Moo::Role;

use namespace::clean;

my %CALL_SITES;

sub _warn_deprecated_method {
    my ( $self, $old, $new ) = @_;

    return if $ENV{PERL_MONGO_NO_DEP_WARNINGS};
    my $trace = _get_trace();
    return unless defined $trace; # already warned from this location

    my $msg = "# The '$old' method will be removed in a future major release.";
    $msg .= _get_alternative($new);

    return __warn_deprecated($msg, $trace);
}

# Expected to be called from BUILD
sub _warn_deprecated_class {
    my ( $self, $old, $new, $uplevel ) = @_;

    return if $ENV{PERL_MONGO_NO_DEP_WARNINGS};

    my $trace = _get_trace(2);
    return unless defined $trace; # already warned from this location

    my $msg = "# The '$old' class will be removed in a future major release.";
    $msg .= _get_alternative($new);

    # fixup name of constructor
    my $class = ref($self);
    $trace =~ s/\S+ called at/${class}::new called at/;

    return __warn_deprecated($msg, $trace);
}

sub __warn_deprecated {
    my ( $msg, $trace ) = @_;
    chomp $msg;
    warn("#\n# *** DEPRECATION WARNING ***\n#\n$msg\n$trace");
    return;
}

sub _get_alternative {
    my ($new) = @_;
    # Arrayref is a list of replacement methods; string is just a message
    if ( ref $new eq 'ARRAY' ) {
        if ( @$new == 1 ) {
            return "\n# Use '$new->[0]' instead.";
        }
        elsif (@$new > 1) {
            my $last = pop @$new;
            my $list = join(", ", map { "'$_'" } @$new);
            return "\n# Use $list or '$last' instead.";
        }
        else {
            return "";
        }
    }
    return "\n # $new" // "";
}

sub _get_trace {
    my ($uplevel) = @_;
    $uplevel //= 0;

    my ( $callsite_found, $pkg, $file, $line, $sub );
    my ( $trace, $i ) = ( "", $uplevel + 1 );

    # Accumulate the stack trace. Start at uplevel + caller(2) to skip
    # '__warn_deprecated' and its internal caller in the stack trace
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

    return $trace;
}


1;
