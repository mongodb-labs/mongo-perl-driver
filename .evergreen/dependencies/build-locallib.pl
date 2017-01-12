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

my @repos = qw(
  mongo-perl-driver
  mongo-perl-bson
  mongo-perl-bson-xs
);

# configure local perl5 user directory

$ENV{PERL5LIB} = "$perl5lib/lib/perl5";
$ENV{PATH}     = "$perl5lib/bin:$ENV{PATH}";
for my $d (qw{bin lib/perl5}) {
    mkpath "$perl5lib/$d" or die "$d: $!";
}

# bootstrap cpanm
try_system("curl -L https://cpanmin.us/ -o $cpanm");
chmod 0755, $cpanm;

# bootstrap known config prereqs
for my $m ( qw/Path::Tiny Config::AutoConf/ ) {
    try_system("cpanm -v --no-interactive -l $perl5lib $m");
}

# install dependencies for repos
for my $r (@repos) {
    chdir "$orig_dir/../$r" or die "chdir to $r: $!";
    try_system("cpanm -v --no-interactive --with-recommends -l $perl5lib --installdeps .");
}

# install known optionals
my @optionals = qw(
    Authen::SASL
    DateTime
    DateTime::Tiny
    IO::Socket::IP
    IO::Socket::SSL
    Mango::BSON::Time
    Math::Int64
    MongoDB
    Mozilla::CA
    Net::SSLeay
    Time::Moment
);

for my $m ( @optionals ) {
    # these are allowed to fail
    eval { try_system("cpanm -v --no-interactive -l $perl5lib $m") };
}

# XXX eventually, install develop requirements (e.g. dzil, etc.)

# tar local lib
chdir "$orig_dir" or die "chdir: $!";
try_system("tar -czf perl5lib.tar.gz perl5");
