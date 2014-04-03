use strict;
use warnings;
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

requires 'Carp';
requires 'Class::MOP::Class';
requires 'DateTime' => '0.78';
requires 'Devel::Size';
requires 'Digest::MD5';
requires 'Encode';
requires 'IO::File';
requires 'Moose' => '2';
requires 'Moose::Util::TypeConstraints' => '2';
requires 'Scalar::Util';
requires 'Tie::IxHash';
requires 'Try::Tiny';
requires 'boolean';
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
test_requires 'Test::Exception';
test_requires 'Test::More';
test_requires 'Test::Warn';
test_requires 'bigint';

mongo;

repository 'git://github.com/mongodb/mongo-perl-driver.git';


if ($Config::Config{useithreads}) {
    tests 't/*.t t/threads/*.t';
}

WriteAll;

package MY;
our $VERSION = '0.45';

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

    if ($Config{use64bitint}) {
        $inherited .= ' -DUSE_64_BIT_INT';
    }
    else {
        $inherited .= ' ';
    }

    return $inherited;
}
