use strict;
use warnings;
use lib 'ext';
use inc::Module::Install;

name 'MongoDB';
all_from 'lib/MongoDB.pm';

requires 'Any::Moose';
# C::M::Modifiers isn't always needed. It'll be used only if Any::Moose decides
# to use Mouse instead of Moose. We depend on it anyway to make sure it's there
# when it's needed.
requires 'Class::Method::Modifiers';
requires 'Digest::MD5';
test_requires 'Test::Exception';

mongo;

no_index directory => 'ext';

license 'apache';
repository 'git://github.com/mongodb/mongo-perl-driver.git';

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
