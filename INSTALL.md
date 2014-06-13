# Installation Instructions for the MongoDB Perl Driver

## Supported platforms

The driver requires Perl v5.8.4 or later for most Unix-like platforms.

The driver may not build successfully on the following platforms:

* Windows
* OpenBSD (single-threaded perls without libpthread compiled in)
* Solaris

We expect to provide support for these platforms in a future release.

## Compiler tool requirements

This module requires `make` and a compiler.

For example, Debian and Ubuntu users should issue the following command:

    $ sudo apt-get install build-essential

Users of Red Hat based distributions (RHEL, CentOS, Amazon Linux, Oracle
Linux, Fedora, etc.) should issue the following command:

    $ sudo yum install make gcc

## Configuration requirements

Configuration requires the following Perl modules:

* Config::AutoConf
* Path::Tiny

If you are using a modern CPAN client (anything since Perl v5.12), these will
be installed automatically as needed.  If you have an older CPAN client or are
doing manual installation, install these before running `Makefile.PL`.

## Testing with a database

Most tests will skip unless a MongoDB database is available either on the
default localhost and port or on an alternate `host:port` specified by the
`MONGOD` environment variable:

    $ export MONGOD=localhosts:31017

## Installing as a non-privileged user

If you do not have write permissions to your Perl's site library directory
(`perl -V:sitelib`), then you will need to use your CPAN client or run
`make install` as root or with `sudo`.

Alternatively, you configure a local library.  See
[local::lib](https://metacpan.org/pod/local::lib#The-bootstrapping-technique)
on CPAN for more details.

## Installing from CPAN

You can install the latest stable release by installing the `MongoDB`
package:

    $ cpan MongoDB

To install a development release, specify it by author and tarball path.
For example:

    $ cpan MONGODB/MongoDB-v0.703.4-TRIAL.tar.gz

## Installing from a tarball downloaded from CPAN

You can install using a CPAN client.  Unpack the tarball and from
inside the unpacked directly, run your CPAN client with `.` as the target:

    $ cpan .

To install manually, first install the configuration requirements listed
above.  Then run the `Makefile.PL` manually:

    $ perl Makefile.PL

This will report any missing prerequisites and you will need to install
them all.  You can then run `make`, etc. as usual:

    $ make
    $ make test
    $ make install

## Installing from the git repository

If you have checked out the git repository (or downloaded a tarball from
Github), you will need to install configuration requirements and follow the
manual procedure described above.

## Building with SSL or SASL support

SSL support requires the libssl-dev package or equivalent.  SASL support
requires libgsasl-dev or equivalent (available from EPEL for Red Hat based
distributions).

To enable SSL, set the `PERL_MONGODB_WITH_SSL` environment variable before
installing.  For example:

    $ PERL_MONGODB_WITH_SSL=1 cpan MongoDB

To enable SASL, set the `PERL_MONGODB_WITH_SASL` environment variable before
installing.  For example:

    $ PERL_MONGODB_WITH_SASL=1 cpan MongoDB

If you are installing manually, these only need to be set when running
`Makefile.PL`.  For example:

    $ PERL_MONGODB_WITH_SASL=1 perl Makefile.PL

Or you can pass the flags `--sasl` or `--ssl` to `Makefile.PL`.

## Non-standard library paths

If your libssl or libgsasl libraries are in a non-standard location, you
will need to pass custom arguments to the `Makefile.PL` using the `LIBS`
parameter.

Due to a quirk in ExtUtils::MakeMaker, this will override any
libraries set by Makefile.PL and you will need to specify them all on the
command line.  You should first run `Makefile.PL` without any arguments and
look in the generated `Makefile` for the `LIBS` parameter in the commented
section at the top.

Then, add your library path and library flags to that and pass it on the
command line.  Be sure your include path is available to your compiler.

For example, assuming libgsasl is installed in /opt/local:

    $ export C_INCLUDE_PATH=/opt/local/include
    $ perl Makefile.PL --sasl LIBS="-L/opt/local/lib -lgsasl -lrt"

The specific list of libraries may be different by platform.

Note: even though you specify the libraries and paths with `LIBS` you will
still need to pass "--ssl" or "--sasl" (or set the corresponding
environment variables) for compiler definitions to be set properly.

