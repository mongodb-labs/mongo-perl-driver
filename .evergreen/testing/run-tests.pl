#!/usr/bin/env perl
use strict;
use warnings;
use Cwd 'getcwd';
use File::Path qw/mkpath rmtree/;

# helper subroutine

sub try_system {
    my @command = @_;
    print "\nRunning: @command\n\n";
    system(@command) and die "Aborting: '@command' failed";
}

# constants

my $orig_dir = getcwd();
my $perl5lib = "$orig_dir/perl5";
my $cpanm    = "$perl5lib/bin/cpanm";

# configure local perl5 user directory

for my $d (qw{bin lib/perl5}) {
    mkpath "$perl5lib/$d";
}
$ENV{PERL5LIB} = "$perl5lib/lib/perl5";
$ENV{PATH}     = "$perl5lib/bin:$ENV{PATH}";

print "PERL5LIB = $ENV{PERL5LIB}\n";
print "PATH = $ENV{PATH}\n";

# Report on current perl (with ENV set)
try_system("perl -V");

# bootstrap cpanm
try_system("curl -L https://cpanmin.us/ -o $cpanm");
chmod 0755, $cpanm;

# install any new, missing dependencies for repos
try_system("cpanm -v --no-interactive --skip-satisfied --with-recommends -l $perl5lib --installdeps .");

# Configure, build
try_system("perl Makefile.PL");
try_system("make");

# XXX $ENV{FAILPOINT_TESTING} = 1;

# Test with asserts
try_system("make test");

# Test without asserts
print "Testing with PERL_MONGO_WITH_ASSERTS = 1\n";
$ENV{PERL_MONGO_WITH_ASSERTS} = 1;

try_system("make test");
