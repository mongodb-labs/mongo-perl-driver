#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use Config;
use Cwd 'getcwd';
use File::Path qw/mkpath rmtree/;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# constants

my $orig_dir      = getcwd();
my $root          = "opt";
my $prefix_prefix = "/$root/perl";
my $destdir       = "$orig_dir/build";
my @default_args  = ( '-j', 16, '-Doptimize=-g' );
my $tardir        = "$destdir/$root";

# define matrix of builds

my @perl_versions = qw(
  5.8.8
  5.10.1
  5.12.5
  5.14.4
  5.16.3
  5.18.4
  5.20.3
  5.22.2
  5.22.4
  5.24.0
  5.24.3
  5.26.1
);

# Build only more recent Perls for ZAP
if ( $Config{archname} =~ /aarch64|s390x|ppc64le/ ) {
    splice @perl_versions, 0, 3;
}

my %config_flags = (
    ''  => '',
    't' => '-Dusethreads',
    'ld' => '-Dusemorebits',
);

# Bootstrap

bootstrap_env();

# Install Perl::Build

run_perl5_cpanm(qw/Perl::Build/);

# build all perls from newest to oldest because older ones might fail
# and we want to see where things stop working
my @logs;
for my $version (reverse @perl_versions) {
    for my $config ( keys %config_flags ) {
        # prepare arguments
        ( my $short_ver = $version ) =~ s/^5\.//;
        my $dest = "$prefix_prefix/$short_ver$config";
        my @args = ( @default_args, $version, $dest );
        unshift @args, $config_flags{$config} if length $config_flags{$config};

        # run perl-build
        local $ENV{DESTDIR} = $destdir;
        my $logfile = "$short_ver$config.log";
        push @logs, $logfile;
        try_system("perl-build @args >$logfile 2>&1");

        # remove man dirs from $destdir$dest/...
        rmtree("$destdir$dest/man");

        # remove most pod files
        my $poddir = "$destdir$dest/lib/$version/pod";
        opendir my $dh, $poddir or die $!;
        my @files = grep { substr( $_, 0, 1 ) ne '.' } readdir($dh);
        for my $file (@files) {
            next if $file eq "perldiag.pod";
            my $fullpath = "$poddir/$file";
            unlink $fullpath or die "While deleting '$fullpath': $!";
        }
    }
}

# tar inside the destdir/opt
try_system("tar -czf perl.tar.gz -C $tardir perl");

# tar the build logs
try_system("tar -czf task-logs.tar.gz @logs");
