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

=head1 NAME

EvergreenConfig

=head1 DESCRIPTION

This module provides helpers for generating 'config.yml' files for
testing MongoDB Perl projects with the Evergreen CI tool.

Read the comments in the file for documentation.

=cut

package EvergreenConfig;

# This config system assumes the following variables are set at the project
# level in Evergreen:
#
# - aws_artifact_prefix -- S3 path for per-commit/patch build artifacts
# - aws_toolchain_prefix -- S3 path prefix for common dependencies
# - aws_key -- S3 credential
# - aws_secret -- S3 credential
# - repo_directory -- name for cloned main repo

use base 'Exporter';

use YAML ();
use List::Util 1.45 qw/uniq/;
use Tie::IxHash;

our @EXPORT = qw(
  assemble_yaml
  buildvariants
  clone
  ignore
  pre
  post
  task
  timeout
);

#--------------------------------------------------------------------------#
# Constants
#--------------------------------------------------------------------------#

my $WEEK_IN_SECS = 7 * 24 * 3600;

{ no warnings 'once'; $YAML::SortKeys = 0; }

# For Unix, we test 5.10.1 to 5.24.0, as this is the full range we support.
# We test default config, plus threaded ("t") and long-double ("ld")
# configs.
my @unix_perls =
  map { $_, "${_}t", "${_}ld" } qw/10 12 14 16 18 20 22 24 26 28/;

# For Windows, we test from 5.14.4 to 5.24.0, as these are available in
# "portable" format.  There are no configuration suffixes; we just use
# the standard Strawberry Perl builds (which happen to be threaded).
my @win_perls = qw/14 16 18 20 22 24 26 28/;

# For Z series, ARM64 and Power8 (aka ZAP), only more recent perls compile
# cleanly, so we test a smaller range of Perls.  Long doubles on Z and
# ARM cause problems in dependencies, we so skip those as well.  Threads
# are already discouraged and tested on x86_64, so we only test vanilla
# perls on ZAP.  We use a threaded perl so that perls before 5.20 that didn't
# automatically link libpthread (even for unthreaded perls) have libpthread.
my @zap_perls = map { "${_}t" } qw/16 18 20 22 24 26 28/;

# The %os_map variable provides details of the full range of MongoDB
# Evergreen operating systems we might run on, plus configuration details
# build and run on each.
#
# Sub-keys include:
# name: Visible display name in Evergreen web pages

# run_on: the MongoDB Evergreen host names; for historical reasons this
# must be an arrayref but must only have a single entry
#
# perlroot: where perls are installed. E.g. /opt/perl or c:/perl
#
# perlpath: dir under perlroot/$version to find perl binary. E.g. 'bin' or 'perl/bin'
#
# perls: a list of perl "versions" (really version plus an optional
# configuration suffix) to use on that OS.
my %os_map = (
    amazon2 => {
        name     => "Amazon v2 x86_64",
        run_on   => [ 'amazon2-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    debian81 => {
        name     => "Debian 8.1 x86_64",
        run_on   => [ 'debian81-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    debian92 => {
        name     => "Debian 9.2 x86_64",
        run_on   => [ 'debian92-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    rhel62 => {
        name     => "RHEL 6.2 x86_64",
        run_on   => [ 'rhel62-small' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    rhel70 => {
        name     => "RHEL 7.0 x86_64",
        run_on   => [ 'rhel70-small' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    suse12 => {
        name     => "SUSE 12 x86_64",
        run_on   => [ 'suse12-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    ubuntu1404 => {
        name     => "Ubuntu 14.04 x86_64",
        run_on   => [ 'ubuntu1404-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    ubuntu1604 => {
        name     => "Ubuntu 16.04 x86_64",
        run_on   => [ 'ubuntu1604-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    ubuntu1804 => {
        name     => "Ubuntu 18.04 x86_64",
        run_on   => [ 'ubuntu1804-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    windows32 => {
        name     => "Win32",
        run_on   => [ 'windows-32' ],
        perlroot => '/cygdrive/c/perl',
        perlpath => 'perl/bin',
        ccpath   => 'c/bin',
        perls    => \@win_perls,
    },
    windows64 => {
        name     => "Win64",
        run_on   => [ 'windows-64-vs2015-test' ],
        perlroot => '/cygdrive/c/perl',
        perlpath => 'perl/bin',
        ccpath   => 'c/bin',
        perls    => \@win_perls,
    },
    rhel67_z => {
        name     => "ZAP RHEL 6.7 Z Series",
        run_on   => [ 'rhel67-zseries-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@zap_perls,
        stepback => 'false',
        batchtime => $WEEK_IN_SECS,
    },
    ubuntu1604_arm64 => {
        name     => "ZAP Ubuntu 16.04 ARM64",
        run_on   => [ 'ubuntu1604-arm64-large' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@zap_perls,
        stepback => 'false',
        batchtime => $WEEK_IN_SECS,
    },
    ubuntu1604_power8 => {
        name     => "ZAP Ubuntu 16.04 Power8",
        run_on   => [ 'ubuntu1604-power8-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@zap_perls,
        stepback => 'false',
        batchtime => $WEEK_IN_SECS,
    },
);

# The %functions variable contains YAML snippets of reusable functions.
#
# The DATA section contains a text file of Evergreen 'function' recipes in
# YAML format.  The DATA section is parsed into individual function
# snippets that can be included on demand in a config.yml
my %functions;
{
    my $current_name;
    my $current_body;
    while ( my $line = <DATA> ) {
        if ( $line =~ m{^"([^"]+)"} ) {
            $functions{$current_name} = $current_body if $current_name;
            ( $current_name, $current_body ) = ( $1, "  $line" );
        }
        else {
            $current_body .= "  $line";
        }
    }
    $functions{$current_name} = $current_body if $current_name;
}

#--------------------------------------------------------------------------#
# Functions
#--------------------------------------------------------------------------#

# assemble_yaml: Given a list of either strings or hash references, concatenate them
# into a single string, converting hash references to YAML.  Effectively,
# this allow assembling a YAML file in pieces, controlling the order of
# sections.

sub assemble_yaml {
    return join "\n", map { _yaml_snippet($_) } _default_headers(), @_;
}

# buildvariants: Given an array reference of 'task' hash refs, return a
# list of hash references representing sections of a config.yml.
#
# Task hash refs allow the following keys:
#
# - name: the name of the task, note that optional 'pre' and 'post' names
# are handled special.  Only one of 'pre' or 'post' can appear. The 'pre'
# task is broken up and manually appended into other tasks (this works
# around a problem where an Evergreen 'pre' task failure doesn't halt a
# task. The 'post' task is provided to Evergreen as a 'post' task there.
#
# - commands: an arrayref of function that make up the task.  Functions
# must either be strings or an array ref with the first value being the
# function name and the second value being a hash-ref of key/value
# arguments to pass to the function.
#
# - depends_on: a list of task names a given task depends upon.
#
# - filter: a hash reference used to determine which tasks are included in
# which variant
#
# - stepback: a boolean, indicating if failures of the task should trigger
# a stepback through commit history to find the failing commit
#
# If a task reference a function not listed in the DATA section, the
# subroutine throws an error.
#
# The hash ref sections returned are the function definitions needed, the
# task definitions, and the build_variant definition.

sub buildvariants {
    my ($tasks, $variant_filter_fcn) = @_;

    # Later, we'll capture function names so we know what snippets to
    # include in the final YAML.
    my (@functions_found);

    # Pull out task names for later verification of dependencies.
    my @task_names = grep { $_ ne 'pre' && $_ ne 'post' } map { $_->{name} } @$tasks;
    my %has_task = map { $_ => 1 } @task_names;

    # Index the filters by task name for later use.
    my %filters;

    # verify the tasks are valid
    for my $t (@$tasks) {
        my @cmds = @{ $t->{commands}   || [] };
        my @deps = @{ $t->{depends_on} || [] };
        $filters{ $t->{name} } = delete $t->{filter};

        my @fcns = map { $_->{func} } @cmds;
        push @functions_found, @fcns;

        my @bad_fcns = grep { !defined $functions{$_} } @fcns;
        die "Unknown function(s): @bad_fcns\n" if @bad_fcns;

        my @bad_deps = grep { !defined $has_task{$_} } map { $_->{name} } @deps;
        die "Unknown dependent task(s): @bad_deps\n" if @bad_deps;
    }

    return (
        _assemble_functions(@functions_found),
        _assemble_tasks($tasks), _assemble_variants( \@task_names, \%filters, $variant_filter_fcn ),
    );
}

# clone: make a deep copy to avoid references

sub clone { return YAML::Load( YAML::Dump(shift) ) }

# ignore: constructs an 'ignore' section for config.yml

sub ignore { return { ignore => [@_] } }

# post: constructs a "post" task hash ref from a list of functions

sub post {
    return { name => 'post', commands => _func_hash_list(@_) };
}

# pre: constructs a "pre" task hash ref from a list of functions

sub pre {
    return { name => 'pre', commands => _func_hash_list(@_) };
}

# task: constructs a task data structure from a name and arrayref of
# commands.  It takes an optional set of arguments.  See buildvariants
# comments for details on task structure.

sub task {
    my ( $name, $commands, %opts ) = @_;
    die "No commands for $name" unless $commands;
    my $task = _hashify( name => $name, commands => _func_hash_list(@$commands) );
    if ( defined( my $deps = $opts{depends_on} ) ) {
        $task->{depends_on} =
          ref $deps eq 'ARRAY' ? _name_hash_list(@$deps) : _name_hash_list($deps);
    }
    $task->{filter} = $opts{filter};
    $task->{stepback} = $opts{stepback} if $opts{stepback};
    return $task;
}

# timeout: constructs a timeout section for evergreen. It has a somewhat
# pointless command it runs rather than anything more thoughtful/useful.

sub timeout {
    my $timeout = shift;
    return () unless $timeout;

    my @parts = (
        { exec_timeout_secs => $timeout },
        {
            timeout => [ _hashify( command => 'shell.exec', params => { script => 'ls -la' } ) ]
        },
    );

    return @parts;
}

# Private functions

# _assemble_functions: takes a list of function names and constructs a YAML
# block of just those functions by stitching together snippets parsed from
# DATA

sub _assemble_functions {
    return join "\n", "functions:", map { $functions{$_} } uniq sort @_;
}

# _assemble_tasks: takes an array ref of task/pre/post structures and
# returns a list of hashrefs wrapped in the correct keys for constructing
# an evergreen config file.  'pre' tasks are copied into each task and then
# omitted as a separate block.

sub _assemble_tasks {
    my $tasks = shift;
    my ( @parts, $pre, $post );
    for my $t (@$tasks) {
        if ( $t->{name} eq 'pre' ) {
            $pre = $t->{commands};
        }
        elsif ( $t->{name} eq 'post' ) {
            $post = $t->{commands};
        }
        else {
            push @parts, $t;
        }
    }

    # 'pre' failures are ignored, so we'll stitch those commands into
    # all tasks directly instead of using Evergreen's 'pre' feature.
    for my $t (@parts) {
        unshift @{ $t->{commands} }, map { _hashify_sorted(%$_) } @{ clone($pre) };
    }

    return ( ( $post ? ( { post => $post } ) : () ), { tasks => [@parts] } );
}

# _assemble_variants: produces a list of build variants with a denormalized
# list of tasks for each variant and other variables a variant requires,
# like variant-specific expansions

sub _assemble_variants {
    my ( $task_names, $filters, $variant_filter_fcn ) = @_;

    my @variants;
    for my $os ( sort keys %os_map ) {
        my $os_map = $os_map{$os};
        for my $ver ( @{ $os_map{$os}{perls} } ) {

            next if $variant_filter_fcn && ! $variant_filter_fcn->($os, $ver);

            # OS specific path to a perl version's PREFIX
            my $prefix_path = "$os_map{$os}{perlroot}/$ver";

            # Paths below the prefix to add to PATH
            my @extra_paths = ( $os_map{$os}{perlpath} );
            push @extra_paths, $os_map{$os}{ccpath} if $os_map{$os}{ccpath};

            # Explicit path to perl to avoid confusion
            my $perlpath = "$prefix_path/$os_map{$os}{perlpath}/perl";

            # Filter out some tasks based on OS and Perl version
            my @filtered = _filter_tasks( $os, $ver, $task_names, $filters );

            # Skip variant if no tasks
            next unless @filtered;

            push @variants,
              _hashify(
                name         => "os_${os}_perl_${ver}",
                display_name => "$os_map{$os}{name} Perl 5.$ver",
                expansions   => _hashify_sorted(
                    os       => $os,
                    perlver  => $ver,
                    perlpath => $perlpath,
                    addpaths => join( ":", map { "$prefix_path/$_" } @extra_paths ),
                ),
                run_on => [ @{ $os_map{$os}{run_on} } ],
                tasks  => [@filtered],
                ( $os_map{$os}{stepback} ? ( stepback => $os_map{$os}{stepback} ) : () ),
                ( $os_map{$os}{batchtime} ? ( batchtime => $os_map{$os}{batchtime} ) : () ),
              );
        }
    }
    return { buildvariants => \@variants };
}

# _default_headers: returns a list of hash refs that should be assembled
# at the top of every config file

sub _default_headers {
    return { stepback => 'true' }, { command_type => 'system' };
}

# _filter_tasks: given OS and Perl version, a list of task names, and a
# hashref of filter parameters per task, returns the list of task names where
# the filters match OS and perl version.  Effectively the filter says "only
# include me if a variant OS and Perl match what I say I'm eligible for".

sub _filter_tasks {
    my ( $os, $ver, $task_names, $filters ) = @_;
    my @filtered;
    for my $t (@$task_names) {
        my $f = $filters->{$t} || {};
        my $os_ok  = $f->{os}   ? ( grep { $os eq $_ } @{ $f->{os} } )       : 1;
        my $ver_ok = $f->{perl} ? ( grep { $ver =~ /^$_/ } @{ $f->{perl} } ) : 1;
        push @filtered, $t
          if $os_ok && $ver_ok;
    }
    return @filtered;
}

# _func_hash_list: given a list of function names or name-variable array refs,
# nest them in a hashref with the key 'func' while also sorting variables.

sub _func_hash_list {
    my @list;
    for my $f (@_) {
        if ( ref $f eq 'ARRAY' ) {
            push @list,
              _hashify_sorted( func => $f->[0], vars => _hashify_sorted( %{ $f->[1] } ) );
        }
        else {
            push @list, { func => $f };
        }
    }
    return \@list;
}

# _hashify: syntactic sugar for constructing order-preserving hashrefs

sub _hashify {
    tie my %hash, "Tie::IxHash", @_;
    return \%hash;
}

# _hashify_sorted: like _hashify, but recursively orders hashref keys

sub _hashify_sorted {
    my %h = @_;
    tie my %hash, "Tie::IxHash";
    for my $k ( sort keys %h ) {
        $hash{$k} = ref( $h{$k} ) eq 'HASH' ? _hashify_sorted( %{ $h{$k} } ) : $h{$k};
    }
    return \%hash;
}

# _name_hash_list: maps names to hashrefs with a 'name' key

sub _name_hash_list {
    return [ map { { name => $_ } } @_ ];
}

# _yaml_snippet: maps hashrefs to YAML string snippets that can be
# concatenated but passes through strings unchanged

sub _yaml_snippet {
    my $data = shift;

    # Passthrough literal text
    return $data unless ref $data;

    my $text = eval { YAML::Dump($data) } || '';
    warn $@ if $@;

    # Remove YAML document divider
    $text =~ s/[^\n]*\n//m;

    return $text;
}

1;

# Evergreen functions in YAML format. This is a cross-project pool
# of functions that can be included in tasks.
__DATA__
"dynamicVars":
  - command: shell.exec
    params:
      script: |
          set -o errexit
          set -o xtrace
          cat <<EOT > expansion.yml
          prepare_shell: |
              export PATH="${addpaths}:$PATH"
              export PERL="${perlpath}"
              export REPO_DIR="${repo_directory}"
              set -o errexit
              set -o xtrace
          EOT
          cat expansion.yml
  - command: expansions.update
    params:
      file: expansion.yml
"whichPerl":
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      $PERL -v
"fetchSource" :
  - command: git.get_project
    params:
      directory: src
  - command: shell.exec
    params:
      script: |
        ${prepare_shell}
        mv src ${repo_directory}
"fetchOtherRepos":
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      git clone https://github.com/mongodb/mongo-perl-bson
      git clone https://github.com/mongodb/mongo-perl-bson-xs
"buildPerl5Lib":
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      TARGET="${target}" $PERL ${repo_directory}/.evergreen/dependencies/build-perl5lib.pl
      ls -l perl5lib.tar.gz
"testPerl5Lib" :
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      $PERL mongo-perl-driver/.evergreen/dependencies/test-perl5lib.pl
"buildModule" :
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      $PERL ${repo_directory}/.evergreen/testing/build.pl
"testDriver" :
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      export MONGOD=$(echo "${MONGODB_URI}" | tr -d '[:space:]')
      export PERL_MONGO_WITH_ASSERTS=${assert}
      export PERL_BSON_BACKEND="${bsonpp}"
      SSL=${ssl} $PERL ${repo_directory}/.evergreen/testing/test.pl
"testLive" :
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      set +x
      echo "export MONGOD=<redacted>"
      export MONGOD="${uri}"
      set -x
      $PERL ${repo_directory}/.evergreen/testing/live-test.pl
"testModule" :
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      $PERL ${repo_directory}/.evergreen/testing/test.pl
"setupOrchestration" :
  - command: shell.exec
    params:
      script: |
        ${prepare_shell}
        VERSION=${version} TOPOLOGY=${topology} AUTH=${auth} SSL=${ssl} $PERL ${repo_directory}/.evergreen/testing/setup-mongo-orchestration.pl
  - command: expansions.update
    params:
      file: mo-expansion.yml
"teardownOrchestration" :
  command: shell.exec
  continue_on_error: true
  params:
    script: |
      ${prepare_shell}
      $PERL ${repo_directory}/.evergreen/testing/teardown-mongo-orchestration.pl
"uploadPerl5Lib":
  command: s3.put
  params:
    aws_key: ${aws_key}
    aws_secret: ${aws_secret}
    local_file: perl5lib.tar.gz
    remote_file: ${aws_toolchain_prefix}/${os}/${perlver}/${target}/perl5lib.tar.gz
    bucket: mciuploads
    permissions: public-read
    content_type: application/x-gzip
"downloadPerl5Lib" :
  - command: s3.get
    params:
      bucket: mciuploads
      aws_key: ${aws_key}
      aws_secret: ${aws_secret}
      remote_file: ${aws_toolchain_prefix}/${os}/${perlver}/${target}/perl5lib.tar.gz
      local_file: perl5lib.tar.gz
  - command: shell.exec
    params:
      script: |
        ${prepare_shell}
        tar -zxf perl5lib.tar.gz
"uploadBuildArtifacts":
  - command: s3.put
    params:
      aws_key: ${aws_key}
      aws_secret: ${aws_secret}
      local_file: ${repo_directory}/build.tar.gz
      remote_file: ${aws_artifact_prefix}/${repo_directory}/${build_id}/build.tar.gz
      bucket: mciuploads
      permissions: public-read
      content_type: application/x-gzip
"downloadBuildArtifacts" :
  - command: s3.get
    params:
      bucket: mciuploads
      aws_key: ${aws_key}
      aws_secret: ${aws_secret}
      remote_file: ${aws_artifact_prefix}/${repo_directory}/${build_id}/build.tar.gz
      local_file: build.tar.gz
  - command: shell.exec
    params:
      script: |
        ${prepare_shell}
        tar -zxf build.tar.gz
"uploadOrchestrationLogs":
  - command: shell.exec
    params:
      script: |
        ${prepare_shell}
        cd driver-tools/.evergreen
        find orchestration -name \*.log | xargs tar czf mongodb-logs.tar.gz
  - command: s3.put
    params:
      aws_key: ${aws_key}
      aws_secret: ${aws_secret}
      local_file: driver-tools/.evergreen/mongodb-logs.tar.gz
      remote_file: ${aws_artifact_prefix}/${build_variant}/${revision}/${version_id}/${build_id}/logs/${task_id}-${execution}-mongodb-logs.tar.gz
      bucket: mciuploads
      permissions: public-read
      content_type: ${content_type|application/x-gzip}
      display_name: "mongodb-logs.tar.gz"
  - command: s3.put
    params:
      aws_key: ${aws_key}
      aws_secret: ${aws_secret}
      local_file: driver-tools/.evergreen/orchestration/server.log
      remote_file: ${aws_artifact_prefix}/${build_variant}/${revision}/${version_id}/${build_id}/logs/${task_id}-${execution}-orchestration.log
      bucket: mciuploads
      permissions: public-read
      content_type: ${content_type|text/plain}
      display_name: "orchestration.log"
"cleanUp":
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      rm -rf ~/.cpanm
      rm -rf perl5
      rm -rf ${repo_directory}
"cleanUpOtherRepos":
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      rm -rf mongo-perl-bson
      rm -rf mongo-perl-bson-xs
"setupAtlasProxy":
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      $PERL ${repo_directory}/.evergreen/testing/setup-atlas-proxy.pl
"testAtlasProxy":
  command: shell.exec
  type: test
  params:
    script: |
      ${prepare_shell}
      export ATLAS_PROXY=1
      export SSL=ssl
      export MONGOD="mongodb://user:pencil@host5.local.10gen.cc:9900/admin?replicaSet=benchmark"
      $PERL ${repo_directory}/.evergreen/testing/test.pl
