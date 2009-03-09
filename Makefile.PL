use strict;
use warnings;
use lib 'ext';
use inc::Module::Install;

name 'Mongo';
all_from 'lib/Mongo.pm';

requires 'Any::Moose';

mongo($ENV{MONGO_SDK});

no_index directory => 'ext';

WriteAll;

package MY;

use Config;

sub const_cccmd {
    my $inherited = shift->SUPER::const_cccmd(@_);
    return '' unless $inherited;

    if ($Config{cc} =~ /^cl\b/) {
        warn 'you are using MSVC... my condolences.';
        $inherited .= ' /Fo$@';
    }
    else {
        $inherited .= ' -o $@';
    }

    return $inherited;
}
