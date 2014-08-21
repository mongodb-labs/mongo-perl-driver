use strict;
use warnings;
use inc::Module::Install;

name 'MongoDB';
perl_version '5.8.4';
author 'Florian Ragwitz <rafl@debian.org>';
author 'Kristina Chodorow <kristina@mongodb.com>';
author 'Mike Friedman <mike.friedman@mongodb.com>';
author 'David.Golden <david.golden@mongodb.com>';
license 'Apache';
all_from 'lib/MongoDB.pm';

requires 'Carp';
requires 'Class::MOP::Class';
requires 'DateTime' => '0.78';
requires 'Digest::MD5';
requires 'Encode';
requires 'IO::File';
requires 'Moose' => '2';
requires 'Moose::Util::TypeConstraints' => '2';
requires 'Safe::Isa';
requires 'Syntax::Keyword::Junction';
requires 'Scalar::Util';
requires 'Throwable';
requires 'Tie::IxHash';
requires 'Time::HiRes';
requires 'Try::Tiny';
requires 'boolean';
requires 'namespace::clean';
requires 'version';

test_requires 'Data::Dump';
test_requires 'Data::Dumper';
test_requires 'Data::Types';
test_requires 'DateTime::Tiny';
test_requires 'Devel::Peek';
test_requires 'File::Copy';
test_requires 'File::Path';
test_requires 'File::Slurp';
test_requires 'File::Spec';
test_requires 'File::Temp' => '0.17';
test_requires 'FileHandle';
test_requires 'JSON' => '2';
test_requires 'Test::Deep';
test_requires 'Test::Fatal';
test_requires 'Test::More' => 0.96;
test_requires 'Test::Warn';
test_requires 'bigint';
test_requires 'YAML::XS';

mongo;

repository 'git://github.com/mongodb/mongo-perl-driver.git';


if ($Config::Config{useithreads}) {
    tests 't/*.t t/threads/*.t';
}

WriteAll;

package MY;

use Config;

# Because we keep our XS in an 'xs' subdirectory, this ensures the object files
# are built there, too, which is needed for linking to work.
# Originally added by Florian Ragwitz, based on Glib::MakeHelper. See
# https://metacpan.org/source/XAOC/Glib-1.304/lib/Glib/MakeHelper.pm#L553
sub const_cccmd {
    my $inherited = shift->SUPER::const_cccmd(@_);
    return '' unless $inherited;

    if ($Config{cc} =~ /^cl\b/) {
        $inherited .= ' /Fo$@';
    }
    else {
        $inherited .= ' -o $@';
    }
    return $inherited;
}
