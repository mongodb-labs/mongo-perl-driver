use strict;
use warnings;
use lib 'ext';
use inc::Module::Install;

name 'MongoDB';
perl_version '5.8.4';
author 'Florian Ragwitz <rafl@debian.org>';
author 'Kristina Chodorow <kristina@10gen.com>';
author 'Mike Friedman <mike.friedman@10gen.com>';
license 'Apache';
all_from 'lib/MongoDB.pm';

my (@cc_lib_links, @cc_optimize_flags);

if ( $ENV{PERL_MONGODB_WITH_SSL} || grep { $_ eq '--ssl' } @ARGV ) {
    push @cc_lib_links, 'ssl', 'crypto';
    push @cc_optimize_flags, '-DMONGO_SSL';
}

if ( $ENV{PERL_MONGODB_WITH_SASL} || grep { $_ eq '--sasl' } @ARGV ) {
    push @cc_lib_links, 'gsasl';
    push @cc_optimize_flags, '-DMONGO_SASL';
}

cc_lib_links( @cc_lib_links ) if @cc_lib_links;
cc_optimize_flags( @cc_optimize_flags ) if @cc_optimize_flags;

requires 'Moose';
# C::M::Modifiers isn't always needed. It'll be used only if Any::Moose decides
# to use Mouse instead of Moose. We depend on it anyway to make sure it's there
# when it's needed.
requires 'Class::Method::Modifiers';
requires 'Digest::MD5';
requires 'Tie::IxHash';
requires 'DateTime';
requires 'XSLoader';
requires 'boolean';
test_requires 'Test::Exception';
test_requires 'Test::Warn';
test_requires 'Tie::IxHash';
test_requires 'DateTime';
test_requires 'boolean';
test_requires 'Data::Types';
test_requires 'File::Slurp';
test_requires 'FileHandle';
test_requires 'JSON';
test_requires 'File::Temp' => '0.17';
test_requires 'Try::Tiny';
test_requires 'DateTime::Tiny';

mongo;

no_index directory => 'ext';

repository 'git://github.com/mongodb/mongo-perl-driver.git';


if ($Config::Config{useithreads}) {
    tests 't/*.t t/threads/*.t';
}

WriteAll;

package MY;
our $VERSION = '0.45';

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

    if ($Config{use64bitint}) {
        $inherited .= ' -DUSE_64_BIT_INT';
    }
    else {
        $inherited .= ' ';
    }

    return $inherited;
}
