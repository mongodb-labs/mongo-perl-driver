#!/usr/bin/env perl
#
#  Copyright 2009-2013 MongoDB, Inc.
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

use v5.10;
use strict;
use warnings;

use lib 'devel/lib';
use MongoDBTest::Orchestrator;

use Getopt::Long;
use Log::Any::Adapter 'Null';
use Path::Tiny;
use Try::Tiny;

my %opts;
GetOptions(
    \%opts,
    'verbose|v',
    'preserve=s',
);

if ( $opts{verbose} ) {
    Log::Any::Adapter->set('Stderr');
}

my ($config_file, @command) = @ARGV;

unless ( $config_file && @command ) {
    die "usage: $0 <config-file> <command> [args ...]\n"
}

if ( ! -f $config_file ) {
    my $new_config = "devel/config/$config_file";
    if ( -f $new_config ) {
        $config_file = $new_config;
    }
    else {
        die "$config_file could not be found\n";
    }
}

say "Creating a deployment from $config_file";

my $orc = MongoDBTest::Orchestrator->new( config_file => $config_file );

try {
    $orc->start;
}
catch {
    say "Problem starting deployment from $config_file.";
    print_log_tails($orc) if $opts{verbose};
    die $_;
};

$ENV{MONGOD} = $orc->as_uri;
say "MONGOD=".$ENV{MONGOD};

say "@command";
my $exit_val = system(@command);
my $signal = $exit_val & 127;
$exit_val = $exit_val >> 8;

print_log_tails($orc) if $opts{verbose} && ($signal || $exit_val);
copy_log_files($orc, $opts{preserve}) if $opts{preserve};

$orc->stop;

exit( $signal || $exit_val );

sub copy_log_files {
    my ($orc, $path) = @_;
    say "Copying logs to $path";
    my $p = path( $path );
    for my $server ( $orc->deployment->all_servers ) {
        my $l = path( $server->logfile );
        eval {
            $p->mkpath;
            $l->copy( $p->child( $server->name() . ".log" ) );
        };
        if ($@) {
            warn "Error copying $l: $@\n";
        }
    }
}

sub print_log_tails {
    my ($orc) = shift;
    for my $server ( $orc->deployment->all_servers ) {
        next unless -e -s $server->logfile;
        say "---------------------------------";
        say "Log tail for " . $server->name . ":";
        my @lines = $server->logfile->lines;
        @lines = splice( @lines, -40 ) if @lines > 40;
        say @lines;
    }
}

__END__

=head1 NAME

harness.pl - run a command under a given deployment definition

=head1 USAGE

    harness.pl <config-file> <command> [args...]

This command will instantiate a deployment from a YAML config file
and run the command with arguments.

The C<MONGOD> environment variable will be set to a MongoDB connection
URI appropriate to the deployment.

=head1 OPTIONS

    --verbose, -v   -- verbose output
    --preserve=DIR  -- copy log files to DIR before shutdown

=cut

# vim: ts=4 sts=4 sw=4 et:
