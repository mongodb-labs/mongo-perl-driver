# Perl Driver Evergreen Continuous Integration

## Files

The files below this directory control our CI system.  Evergreen allows
multiple 'projects' within a repository; a project is controlled by a
`config.yml` file, which can execute other commands.  Our CI system uses
two project directories for building perls and prerequisites:

* toolchain – this directory contains configuration and support files for
  building and/or installing perls from source for different architectures
* dependencies – this directory contains configuration and support files
  for building an architecture-specific 'local PERL5LIB' tarball specific
  to the requirements for our three Perl projects: mongo-perl-driver,
  mongo-perl-bson, and mongo-perl-bson-xs

We build prerequisites in a local PERL5LIB tarball to avoid having every CI
run spend minutes downloading and installing dependencies from CPAN.

We have another directory for actually testing the driver.  This is
designed so that it can be 'rsynced' to older branches and work without
modification.

* testing – this directory contains configuration and support for testing
  the driver, typically under various MongoDB orchestration scenarios (e.g.
  'MongoDB 3.2 replica set with SSL enabled')

The `dependencies` and `testing` projects depend upon common modules in the
`lib` directory, both for building a `config.yml` and during a CI run.

## Project variables

Config files depend on the following variables being set in the
Evergreen project configuration:

* aws_artifact_prefix – a directory name for per-commit artifacts
* aws_toolchain_prefix – a directory name for artifacts shared across
  projects (like PERL5LIB tarballs)
* aws_key – S3 credential
* aws_secret - S3 credential
* repo_directory – the name of the git repository the config applies to

## Approach

### Getting platform-specific Perls

For Unix perls, we build Perls from source in three variations: standard
(without threads), with threads, and with long-doubles.  These are placed
in a standardized version-architecture-specific layout in `/opt/perl`.
A tarball of this is then uploaded to be deployed onto Evergreen hosts.

For Windows perl, we download portable [Strawberry
Perl](http://strawberryperl.com/) binaries and assemble them into a
standard directory layout under a directory called `perl`.  A tarball of
this is then uploaded to be deployed as `C:\perl` onto Evergreen hosts.

### Dependencies and testing projects

For `dependencies` and `testing`, we build a CI matrix covering the
various versions of perl built in the `tooclhain` project.

Rather than use Evergreen's limited matrix capabilities, both projects
generate a denormalized `config.yml` with the `write-config.pl` program in
each directory.  The resulting config file is checked into the repository.

### Orchestration and the matrix of combinations

For testing, we test a small subset of the possible matrix of combinations
of perl versions, architectures, server versions and server configurations:

* For an "old" and a "new" perl (unthreaded only) on each platform, we test
  all server version/configuration combination
* For all other perl version/configuration combinations, we test just a
  standalone server of each supported server version

This gives relative good visibility into the causes of failure using only a
small subset of resources needed to test the full matrix.
