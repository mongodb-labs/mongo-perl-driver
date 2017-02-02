use 5.008001;
use strict;
use warnings;

package EvergreenHelper;

use Config;
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

# constants

my $orig_dir = getcwd();
my $path_sep = $Config{path_sep};
my $perl5lib = "$orig_dir/perl5";
my $cpanm    = "$orig_dir/cpanm";

# configure local perl5 user directory

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

sub configure { try_system( $^X, "Makefile.PL" ) }

sub filter_file {
    my ( $file, $code ) = @_;
    local $_ = slurp($file);
    $code->();
    spew( $file, $_ );
}

sub fix_config_files_in {
    my $dir   = shift;
    my $fixer = sub {
        return unless -f && /\.json$/;
        filter_file( $_, sub { s/ABSOLUTE_PATH_REPLACEMENT_TOKEN/$dir/g } );
    };
    find( $fixer, $dir );
}

sub fix_shell_files_in {
    my $dir   = shift;
    my $fixer = sub {
        return unless -f && /\.sh$/;
        filter_file( $_, sub { s/\r//g } );
        chmod 0755, $_ or die chmod "$_: $!";
    };
    find( $fixer, $dir );
}

sub fwd_slash {
    my $path = shift;
    $path =~ tr[\\][/];
    return $path;
}

sub get_info {
    print "PATH = $ENV{PATH}\n";
    # perl -V prints all env vars starting with "PERL"
    try_system(qw(perl -V));
}

sub make { try_system( $Config{make}, @_ ) }

sub maybe_prepend_env {
    my ( $key, $value ) = @_;
    my $orig = $ENV{$key};
    return
      if defined $orig
      && ( $orig =~ m{ (?: ^ | $path_sep ) $value (?: $ | $path_sep ) }x );
    my @orig = defined $ENV{$key} ? ( $ENV{$key} ) : ();
    $ENV{$key} = join( $path_sep, $value, @orig );
}

sub prepend_env {
    my ( $key, @list ) = @_;
    my @orig = defined $ENV{$key} ? ( $ENV{$key} ) : ();
    return join( $path_sep, @list, @orig );
}

sub run_in_dir {
    my ( $dir, $code ) = @_;
    my $start_dir = getcwd();
    my $guard = Local::TinyGuard->new( sub { chdir $start_dir } );
    chdir $dir or die "chdir $dir: $!\n";
    $code->();
}

sub run_local_cpanm {
    my @args     = @_;
    my $locallib = getcwd() . "/local";
    bootstrap_locallib($locallib);
    try_system( $^X, '--', $cpanm,
        qw( -v --no-interactive --skip-satisfied --with-recommends -l ),
        $locallib, @args );
}

sub run_perl5_cpanm {
    my @args = @_;
    try_system( $^X, '--', $cpanm,
        qw( -v --no-interactive --skip-satisfied --with-recommends -l ),
        $perl5lib, @args );
}

sub slurp {
    my ($file) = @_;
    open my $fh, "<:raw", $file or die "$file: $!";
    return scalar do { local $/; <$fh> };
}

sub spew {
    my ( $file, @data ) = @_;
    open my $fh, ">:raw", $file or die "$file: $!";
    print {$fh} $_ for @data;
    close $fh or die "closing $file: $!";
    return 1;
}

sub try_system {
    my @command = @_;
    print "\nRunning: @command\n\n";
    system(@command) and die "Aborting: '@command' failed";
}

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
