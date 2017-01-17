#!/usr/bin/env perl
use v5.10;
use strict;
use warnings;
use utf8;
use open qw/:std :utf8/;

# Get helpers
use FindBin qw($Bin);
use lib "$Bin/../lib";
use EvergreenConfig;

sub main {
    my $prefix = do { local $/; <DATA> };

    my $generated_tasks = generate_tasks();
    my $tasks    = yaml_snippet( { tasks         => $generated_tasks } );
    my @task_list = map { $_->{name} } @$generated_tasks;

    my $variants = yaml_snippet( { buildvariants => generate_variants(\@task_list) } );

    print join( "\n", $prefix, $tasks, $variants ) . "\n";

    return 0;
}

sub compile_task {
    return {
        name     => 'compile-driver',
        commands => [ { func => 'compilePerlDriver' }, { func => 'uploadBuildArtifacts' }, ],
    };
}

sub create_test_task {
    my ( $name, $depends, $orchestration ) = @_;
    die "Test '$name' needs a dependency\n" unless $depends;
    return {
        name       => $name,
        depends_on => [ { name => $depends } ],
        commands   => [
            ( $orchestration ? @$orchestration : () ),
            { func => 'downloadBuildArtifacts' },
            { func => 'testPerlDriver' },
        ],
    };
}

sub generate_tasks {
    return [ compile_task(), create_test_task( "unit-test", "compile-driver" ), ];
}

# execution
exit main();

# The DATA section includes the first part of the YAML file, which does
# not need to be generated dynamically.  Update globals and function
# definitions here.  Tasks and build variants are generated dynamically.

__DATA__
# Ignore changes to toolchain and dependencies evergreen files
ignore:
    - "/.evergreen/toolchain/*"
    - "/.evergreen/dependencies/*"

# When a task that used to pass starts to fail
# Go through all versions that may have been skipped to detect
# when the task started failing
stepback: true

# Mark a failure as a system/bootstrap failure (purple box) rather then a task
# failure by default.
# Actual testing tasks are marked with `type: test`
command_type: system

# Protect ourself against rogue test case, or curl gone wild, that runs forever
# Good rule of thumb: the averageish length a task takes, times 5
# That roughly accounts for variable system performance for various buildvariants
exec_timeout_secs: 1800

# What to do when evergreen hits the timeout (`post:` tasks are run automatically)
timeout:
  - command: shell.exec
    params:
      script: |
        ls -la

# FUNCTIONS
functions:
  "dynamicVars":
    - command: shell.exec
      params:
        script: |
            set -o errexit
            set -o xtrace
            export ADDPATHS="${addpaths}"
            export PERL="${perlpath}"
            export PROJECT_DIRECTORY="$(pwd)"
            export ARTIFACT_BUCKET="mongo-perl-driver"
            export TOOLS_BUCKET="perl-driver-toolchain"

            cat <<EOT > expansion.yml
            PERL: "$PERL"
            ADDPATHS: "$ADDPATHS"
            PROJECT_DIRECTORY: "$PROJECT_DIRECTORY"
            ARTIFACT_BUCKET: "$ARTIFACT_BUCKET"
            TOOLS_BUCKET: "$TOOLS_BUCKET"
            PREPARE_SHELL: |
                set -o errexit
                set -o xtrace
                export PERL="$PERL"
                export PATH="$ADDPATHS:$PATH"
                export PROJECT_DIRECTORY="$PROJECT_DIRECTORY"
            EOT
            cat expansion.yml
    - command: expansions.update
      params:
        file: expansion.yml
  "fetchSource" :
    command: git.get_project
    params:
      directory: mongo-perl-driver
  "downloadPerl5Lib" :
    command: shell.exec
    params:
      script: |
        ${PREPARE_SHELL}
        curl https://s3.amazonaws.com/mciuploads/${TOOLS_BUCKET}/${os}/${perlver}/perl5lib.tar.gz -o perl5lib.tar.gz --fail --show-error --silent --max-time 240
        tar -zxf perl5lib.tar.gz
  "compilePerlDriver" :
    command: shell.exec
    type: test
    params:
      script: |
        ${PREPARE_SHELL}
        $PERL mongo-perl-driver/.evergreen/testing/compile-driver.pl
  "uploadBuildArtifacts":
    - command: s3.put
      params:
        aws_key: ${aws_key}
        aws_secret: ${aws_secret}
        local_file: mongo-perl-driver/build.tar.gz
        remote_file: ${ARTIFACT_BUCKET}/${build_variant}/${revision}/${task_name}/${build_id}.tar.gz
        bucket: mciuploads
        permissions: public-read
        content_type: application/x-gzip
  "downloadBuildArtifacts" :
    command: shell.exec
    params:
      script: |
        ${PREPARE_SHELL}
        chdir mongo-perl-driver
        curl https://s3.amazonaws.com/mciuploads/${ARTIFACT_BUCKET}/${build_variant}/${revision}/${task_name}/${build_id}.tar.gz -o build.tar.gz --fail --show-error --silent --max-time 240
        tar -zxf build.tar.gz
  "testPerlDriver" :
    command: shell.exec
    type: test
    params:
      script: |
        ${PREPARE_SHELL}
        $PERL mongo-perl-driver/.evergreen/testing/run-tests.pl
  "cleanUp":
    command: shell.exec
    params:
      script: |
        ${PREPARE_SHELL}
        rm -rf perl5
        rm -rf mongo-perl-driver

# PRE/POST TASKS
pre:
  - func: dynamicVars
  - func: cleanUp
  - func: fetchSource
  - func: downloadPerl5Lib

post:
  - func: cleanUp
