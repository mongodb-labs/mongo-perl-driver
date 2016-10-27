# Installation Instructions for the MongoDB Perl Driver

This file describes requirements and procedures for installing the MongoDB
Perl driver, typically from CPAN or a tarball.  To work on the code in the
repository, see the [CONTRIBUTING.md](CONTRIBUTING.md) file instead.

## Supported platforms

The driver requires Perl v5.8.4 or later for most Unix-like platforms.

It is known to build successfully on the following operating systems:

* Linux
* FreeBSD, OpenBSD, NetBSD
* Mac OSX
* Windows Vista/2008+ with Strawberry Perl 5.14 or later

Please see the [CPAN Testers Matrix](http://matrix.cpantesters.org/?dist=MongoDB)
for more details on platform/perl compatibility.

The driver has not been tested on big-endian platforms.  Big-endian
platforms will require Perl 5.10 or later.

## Compiler tool requirements

This module requires `make` and a compiler.

For example, Debian and Ubuntu users should issue the following command:

    $ sudo apt-get install build-essential

Users of Red Hat based distributions (RHEL, CentOS, Amazon Linux, Oracle
Linux, Fedora, etc.) should issue the following command:

    $ sudo yum install make gcc

On Windows, [StrawberryPerl](http://strawberryperl.com/) ships with a
GCC compiler.

## Configuration requirements

Configuration requires the following Perl modules:

* Config::AutoConf
* Path::Tiny

If you are using a modern CPAN client (anything since Perl v5.12), these will
be installed automatically as needed.  If you have an older CPAN client or are
doing manual installation, install these before running `Makefile.PL`.

    $ cpan Config::AutoConf Path::Tiny

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

    $ cpan MONGODB/MongoDB-v0.999.999.4-TRIAL.tar.gz

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

## SSL and/or SASL support

SSL support requires installing the
[IO::Socket::SSL](http://p3rl.org/IO::Socket::SSL) module.   You will need
to have the libssl-dev package or equivalent installed for that to build
successfully.

SASL support requires [Authen::SASL](http://p3rl.org/Authen::SASL) and
possibly a Kerberos-capable backend.

The [Authen::SASL::Perl](http://p3rl.org/Authen::SASL::Perl) backend comes
with Authen::SASL and requires the [GSSAPI](http://p3rl.org/GSSAPI) CPAN
module for GSSAPI support.

Installing the GSSAPI module from CPAN rather than an OS package requires
libkrb5 and the krb5-config utility (available for Debian/RHEL systems in
the libkrb5-dev or equivalent package).

Alternatively, the [Authen::SASL::XS](http://p3rl.org/Authen::SASL::XS)
or [Authen::SASL::Cyrus](http://p3rl.org/Authen::SASL::Cyrus) modules
may be used.  Both rely on Cyrus libsasl. Authen::SASL::XS is
preferred.  Installing Authen::SASL::XS or Authen::SASL::Cyrus from CPAN
requires libsasl.  On Debian systems, it is available from libsasl2-dev; on
RHEL, it is available in cyrus-sasl-devel.

