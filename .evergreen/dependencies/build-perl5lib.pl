#!/usr/bin/env perl
use strict;
use warnings;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenHelper;

# Bootstrap

bootstrap_env();

# Install known config prereqs

run_perl5_cpanm(qw/Path::Tiny Config::AutoConf/);

# Install repo dependencies

my @repos = qw(
  mongo-perl-driver
  mongo-perl-bson
  mongo-perl-bson-xs
);

for my $r (@repos) {
    run_in_dir $r => sub { run_perl5_cpanm(qw/--installdeps ./) };
}

# Install known optionals

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

for my $m (@optionals) {
    # these are allowed to fail individually
    eval { run_perl5_cpanm($m) };
}

# XXX eventually, install develop requirements (e.g. dzil, etc.)

# tar local lib
try_system(qw(tar -czf perl5lib.tar.gz perl5));
