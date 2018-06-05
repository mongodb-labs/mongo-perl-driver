#!/usr/bin/env perl
#
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

# Type-Tiny sometimes causes weird segfaults in one, non-functional
# test on Windows, so if it fails, install without testing.
if ($^O eq 'MSWin32') {
    # try normally first
    eval { run_perl5_cpanm("Type::Tiny") };
    # install without tests
    if ( $@ ) {
        run_perl5_cpanm("-n", "Type::Tiny");
    }
}

# Install repo dependencies

my $repo = $ENV{TARGET};

die "No such directory '$repo'\n" unless -d $repo;

run_in_dir $repo => sub { run_perl5_cpanm(qw/--installdeps ./) };

# Install known optionals only for mongo-perl-driver

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
