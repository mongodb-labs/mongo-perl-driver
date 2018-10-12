#  Copyright 2017 - present MongoDB, Inc.
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

use 5.008001;
use strict;
use warnings;

package EvergreenHelper;

use Config;
use Carp 'croak';
use Cwd 'getcwd';
use File::Find qw/find/;
use File::Path qw/mkpath rmtree/;
use base 'Exporter';

our @EXPORT = qw(
  bootstrap_env
  bootstrap_locallib
  configure
  filter_file
  fix_config_files_in
  fix_shell_files_in
  fwd_slash
  get_info
  make
  maybe_prepend_env
  prepend_env
  run_in_dir
  run_local_cpanm
  run_perl5_cpanm
  slurp
  spew
  try_system
);

#--------------------------------------------------------------------------#
# constants
#--------------------------------------------------------------------------#

my $orig_dir = getcwd();
my $path_sep = $Config{path_sep};
my $perl5lib = "$orig_dir/perl5";
my $cpanm    = "$orig_dir/cpanm";

#--------------------------------------------------------------------------#
# functions
#--------------------------------------------------------------------------#

# bootstrap_env: bootstrap local libs and cpanm and clean up environment

sub bootstrap_env {
    # bootstrap general perl local library
    bootstrap_locallib($perl5lib);

    # SHELL apparently causes trouble on MSWin32 perl launched from cygwin
    if ( $^O eq 'MSWin32' ) {
        delete $ENV{SHELL};
    }

    # bootstrap cpanm
    unlink 'cpanm';
    try_system(qw(curl -L https://cpanmin.us/ --fail --show-error --silent -o cpanm));
}

# bootstrap_locallib: configure a local perl5 user directory in a path

sub bootstrap_locallib {
    my $path = shift;

    for my $d (qw{bin lib/perl5}) {
        mkpath "$path/$d";
    }

    maybe_prepend_env( PERL5LIB => "$path/lib/perl5" );
    maybe_prepend_env( PATH     => "$path/bin" );

    require lib;
    lib->import("$path/lib/perl5");
}

# configure: run Makefile.PL

sub configure { try_system( $^X, "Makefile.PL" ) }

# filter_file: given a path and a coderef that modifies $_, replace the
# file with the modified contents

sub filter_file {
    my ( $file, $code ) = @_;
    local $_ = slurp($file);
    $code->();
    spew( $file, $_ );
}

# fix_config_files_in: given a directory of mongo orchestration config
# files, modify .json files to replace certain tokens

sub fix_config_files_in {
    my $dir   = shift;
    my $fixer = sub {
        return unless -f && /\.json$/;
        filter_file( $_, sub { s/ABSOLUTE_PATH_REPLACEMENT_TOKEN/$dir/g } );
    };
    find( $fixer, $dir );
}

# fix_shell_files_in: given a directory with .sh files, clean them up
# by removing CRs and fixing permissions

sub fix_shell_files_in {
    my $dir   = shift;
    my $fixer = sub {
        return unless -f && /\.sh$/;
        filter_file( $_, sub { s/\r//g } );
        chmod 0755, $_ or croak chmod "$_: $!";
    };
    find( $fixer, $dir );
}

# fwd_slash: change a path to have forward slashes

sub fwd_slash {
    my $path = shift;
    $path =~ tr[\\][/];
    return $path;
}

# get_info: print PATH and perl-V info

sub get_info {
    print "PATH = $ENV{PATH}\n";
    # perl -V prints all env vars starting with "PERL"
    try_system(qw(perl -V));
}

# make: run 'make' or 'dmake' or whatever

sub make { try_system( $Config{make}, @_ ) }

# maybe_prepend_env: prepends a value to an ENV var if it doesn't exist.
# This is currently hardcoded for PATH separators.

sub maybe_prepend_env {
    my ( $key, $value ) = @_;
    my $orig = $ENV{$key};
    return
      if defined $orig
      && ( $orig =~ m{ (?: ^ | $path_sep ) $value (?: $ | $path_sep ) }x );
    my @orig = defined $ENV{$key} ? ( $ENV{$key} ) : ();
    $ENV{$key} = join( $path_sep, $value, @orig );
}

# prepend_env: unconditionally prepend a value to an ENV var

sub prepend_env {
    my ( $key, @list ) = @_;
    my @orig = defined $ENV{$key} ? ( $ENV{$key} ) : ();
    return join( $path_sep, @list, @orig );
}

# run_in_dir: given a directory and code ref, temporarily change to that
# directory for the duration of the code

sub run_in_dir {
    my ( $dir, $code ) = @_;
    my $start_dir = getcwd();
    my $guard = Local::TinyGuard->new( sub { chdir $start_dir } );
    chdir $dir or croak "chdir $dir: $!\n";
    $code->();
}

# run_local_cpanm: run cpanm and install to a 'local' perl5lib

sub run_local_cpanm {
    my @args     = @_;
    my $locallib = getcwd() . "/local";
    bootstrap_locallib($locallib);
    try_system( $^X, '--', $cpanm,
        qw( -v --no-lwp --no-interactive --skip-satisfied --with-recommends -l ),
        $locallib, @args );
}

# run_perl5_cpanm: run cpanm and install to 'main' perl5lib

sub run_perl5_cpanm {
    my @args = @_;
    try_system( $^X, '--', $cpanm,
        qw( -v --no-lwp --no-interactive --skip-satisfied --with-recommends -l ),
        $perl5lib, @args );
}

# slurp: full file read

sub slurp {
    my ($file) = @_;
    open my $fh, "<:raw", $file or croak "$file: $!";
    return scalar do { local $/; <$fh> };
}

# spew: full file write; NOT ATOMIC

sub spew {
    my ( $file, @data ) = @_;
    open my $fh, ">:raw", $file or croak "$file: $!";
    print {$fh} $_ for @data;
    close $fh or croak "closing $file: $!";
    return 1;
}

# try_system: print and run a command and croak if exit code is non-zero

sub try_system {
    my @command = @_;
    print "\nRunning: @command\n\n";
    system(@command) and croak "Aborting: '@command' failed";
}

# Local::TinyGuard -- an object that runs a closure on destruction

package Local::TinyGuard;

sub new {
    my ( $class, $code ) = @_;
    return bless $code, $class;
}

sub DESTROY {
    my $self = shift;
    $self->();
}

1;
