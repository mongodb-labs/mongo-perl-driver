use 5.008001;
use strict;
use warnings;

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

# Constants

{ no warnings 'once'; $YAML::SortKeys = 0; }

my @unix_perls =
  map { $_, "${_}t", "${_}ld" } qw/10.1 12.5 14.4 16.3 18.4 20.3 22.2 24.0/;
my @win_perls = qw/ 14.4 16.3 18.4 20.3 22.2 24.0/;

my @win_dists = (
    ( map { ; "windows-64-$_-compile", "windows-64-$_-test" } qw/vs2010 vs2013/ ),
    ( map { ; "windows-64-vs2015-$_" } qw/compile test large/ )
);

my @zap_perls = map { $_, "${_}t", "${_}ld" } qw/14.4 16.3 18.4 20.3 22.2 24.0/;

# perlroot: where perls are installed. E.g. /opt/perl or c:/perl
# binpath: dir under perlroot/$version to find perl binary. E.g. 'bin' or 'perl/bin'
my %os_map = (
    ubuntu1604 => {
        name     => "Ubuntu 16.04",
        run_on   => [ 'ubuntu1604-test', 'ubuntu1604-build' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    rhel62 => {
        name     => "RHEL 6.2",
        run_on   => [ 'rhel62-test', 'rhel62-build', 'rhel62-large' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@unix_perls,
    },
    windows64 => {
        name     => "Win64",
        run_on   => \@win_dists,
        perlroot => '/cygdrive/c/perl',
        perlpath => 'perl/bin',
        ccpath   => 'c/bin',
        perls    => \@win_perls,
    },
    suse12_z => {
        name     => "SUSE 12 Z Series",
        run_on   => [ 'suse12-zseries-build', 'suse12-zseries-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@zap_perls,
    },
    ubuntu1604_arm64 => {
        name     => "Ubuntu 16.04 ARM64",
        run_on   => [ 'ubuntu1604-arm64-large', 'ubuntu1604-arm64-small' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@zap_perls,
    },
    ubuntu1604_power8 => {
        name     => "Ubuntu 16.04 Power8",
        run_on   => [ 'ubuntu1604-power8-build', 'ubuntu1604-power8-test' ],
        perlroot => '/opt/perl',
        perlpath => 'bin',
        perls    => \@zap_perls,
    },
);

# Load functions from DATA
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

# Functions

sub assemble_yaml {
    return join "\n", map { _yaml_snippet($_) } _default_headers(), @_;
}

sub buildvariants {
    my ($tasks) = @_;
    my (@functions_found);

    # Pull out task names for later verification of dependencies.
    # Also pull out filters for user later in assembly.
    my @task_names = grep { $_ ne 'pre' && $_ ne 'post' } map { $_->{name} } @$tasks;
    my %has_task = map { $_ => 1 } @task_names;
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

    # pull out task filters

    # assemble the list of functions
    return (
        _assemble_functions(@functions_found),
        _assemble_tasks($tasks), _assemble_variants( \@task_names, \%filters ),
    );
}

sub clone { return YAML::Load( YAML::Dump(shift) ) }

sub ignore { return { ignore => [@_] } }

sub post {
    return { name => 'post', commands => _func_hash_list(@_) };
}

sub pre {
    return { name => 'pre', commands => _func_hash_list(@_) };
}

sub task {
    my ( $name, $commands, %opts ) = @_;
    die "No commands for $name" unless $commands;
    my $task = _hashify( name => $name, commands => _func_hash_list(@$commands) );
    if ( defined( my $deps = $opts{depends_on} ) ) {
        $task->{depends_on} =
          ref $deps eq 'ARRAY' ? _name_hash_list(@$deps) : _name_hash_list($deps);
    }
    $task->{filter} = $opts{filter};
    return $task;
}

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

sub _assemble_functions {
    return join "\n", "functions:", map { $functions{$_} } uniq sort @_;
}

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

sub _assemble_variants {
    my ( $task_names, $filters ) = @_;

    my @variants;
    for my $os ( sort keys %os_map ) {
        my $os_map = $os_map{$os};
        for my $ver ( @{ $os_map{$os}{perls} } ) {
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
                display_name => "$os_map{$os}{name} Perl $ver",
                expansions   => _hashify_sorted(
                    os       => $os,
                    perlver  => $ver,
                    perlpath => $perlpath,
                    addpaths => join( ":", map { "$prefix_path/$_" } @extra_paths ),
                ),
                run_on => [ @{ $os_map{$os}{run_on} } ],
                tasks  => [@filtered],
              );
        }
    }
    return { buildvariants => \@variants };
}

sub _default_headers {
    return { stepback => 'true' }, { command_type => 'system' };
}

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

sub _hashify {
    tie my %hash, "Tie::IxHash", @_;
    return \%hash;
}

sub _hashify_sorted {
    my %h = @_;
    tie my %hash, "Tie::IxHash";
    for my $k ( sort keys %h ) {
        $hash{$k} = ref( $h{$k} ) eq 'HASH' ? _hashify_sorted( %{ $h{$k} } ) : $h{$k};
    }
    return \%hash;
}

sub _name_hash_list {
    return [ map { { name => $_ } } @_ ];
}

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
      SSL=${ssl} $PERL ${repo_directory}/.evergreen/testing/test.pl
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
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      curl https://s3.amazonaws.com/mciuploads/${aws_toolchain_prefix}/${os}/${perlver}/${target}/perl5lib.tar.gz -o perl5lib.tar.gz --fail --show-error --silent --max-time 240
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
  command: shell.exec
  params:
    script: |
      ${prepare_shell}
      cd ${repo_directory}
      curl https://s3.amazonaws.com/mciuploads/${aws_artifact_prefix}/${repo_directory}/${build_id}/build.tar.gz -o build.tar.gz --fail --show-error --silent --max-time 240
      tar -zxmf build.tar.gz
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
