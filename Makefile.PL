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
test_requires 'Test::Exception';

# Look for libmongoclient and .h files in standard locations
my $haveFileSpec;
sub SearchFor {
  my($subdir, @files) = @_;
  $haveFileSpec = eval { require File::Spec } unless defined($haveFileSpec);

  my @dirs = ($^O =~ /mswin32/i) ? qw(C:) : qw(/usr/local /usr /opt);

  for my $f (@files) {
    for my $dir (@dirs) {
      my $try1 = $haveFileSpec ?
	File::Spec->catdir($dir, $subdir) : "$dir/$subdir";
      my $try2 = $haveFileSpec ?
	File::Spec->catdir($dir, "mongo") : "$dir/mongo";
      my $try3 = $haveFileSpec ?
	File::Spec->catdir($try1, "mongo") : "$try1/mongo";
      my $try4 = $haveFileSpec ?
	File::Spec->catdir($try2, $subdir) : "$try2/$subdir";
      for my $path ($try3, $try4, $try2, $try1, $dir) {
	my $file = $haveFileSpec ?
	  File::Spec->catfile($path, $f) : "$path/$f";
	if (-f $file) {
	  return $path;
	}
      }
    }
  }
}

if (exists $ENV{MONGO_SDK} && -d $ENV{MONGO_SDK}) {
    mongo($ENV{MONGO_SDK});
}
elsif (exists $ENV{MONGO_INC} && -d $ENV{MONGO_INC} &&
         exists $ENV{MONGO_LIB} && -d $ENV{MONGO_LIB}) {
    mongo($ENV{MONGO_INC}, $ENV{MONGO_LIB});
}
else {
    my $includeDir = SearchFor('include', File::Spec->catdir('mongo', 'targetver.h'));
    my $libDir = SearchFor('lib', 'libmongoclient.a');

    if(-d $includeDir && -d $libDir) {
        mongo($includeDir, $libDir);
    }
    else {
        print STDERR <<'ERR';

The MONGO_SDK environment variable isn't set or it doesn't point to a
mongodb build. Cannot continue.

Please MONGO_SDK to point to your MongoDB build and re-run Makefile.PL.
ERR
        exit 0;
    }
}


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
