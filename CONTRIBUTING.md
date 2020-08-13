# END OF LIFE NOTICE

Version v2.2.0 was the final feature release of the MongoDB Perl driver and
version v2.2.2 is the final patch release.

**As of August 13, 2020, the MongoDB Perl driver and related libraries have
reached end of life and are no longer supported by MongoDB.** See the [August
2019 deprecation
notice](https://www.mongodb.com/blog/post/the-mongodb-perl-driver-is-being-deprecated)
for rationale.

If members of the community wish to continue development, they are welcome to
fork the code under the terms of the Apache 2 license and release it under a
new namespace.  Specifications and test files for MongoDB drivers and
libraries are published in an open repository:
[mongodb/specifications](https://github.com/mongodb/specifications/tree/master/source).

# Introduction

`mongo-perl-driver` is the official client-side driver for talking to
MongoDB with Perl.  It is free software released under the Apache 2.0
license and available on CPAN under the distribution name `MongoDB`.

This file describes requirements and procedures for developing and testing the
MongoDB Perl driver from its code repository.  For instructions installing
from CPAN or tarball, see the [INSTALL.md](INSTALL.md) file instead.

While this distribution is shipped using Dist::Zilla, you do not need to
install it or use it for development and testing.

# Working with the source

## Compiler tool requirements

This module requires `make` and a compiler.

For example, Debian and Ubuntu users should issue the following command:

    $ sudo apt-get install build-essential

Users of Red Hat based distributions (RHEL, CentOS, Amazon Linux, Oracle
Linux, Fedora, etc.) should issue the following command:

    $ sudo yum install make gcc

On Windows, [StrawberryPerl](http://strawberryperl.com/) ships with a
GCC compiler.

On Mac, install XCode or just the [XCode command line
tools](https://developer.apple.com/library/ios/technotes/tn2339/_index.html).

## Installing Perl dependencies as a non-privileged user

If you do not have write permissions to your Perl's site library directory
(`perl -V:sitelib`), then you will need to use your CPAN client or run
`make install` as root or with `sudo`.

Alternatively, you can configure a local library.  See
[local::lib](https://metacpan.org/pod/local::lib#The-bootstrapping-technique)
on CPAN for more details.  If you configure a local library, don't forget
to modify your `.bashrc` or equivalent files.

## Configuration and dependencies

You will need to install Config::AutoConf and Path::Tiny to be able to run
the Makefile.PL.

    $ cpan Config::AutoConf Path::Tiny

To configure:

    $ perl Makefile.PL

The output will highlight any missing dependencies.  Install those with the
`cpan` client.

    $ cpan [list of dependencies]

You may also use `cpan` to install the current stable MongoDB driver with
`cpan MongoDB`, which should pick up most of the dependencies you will
need automatically.

## Building and testing

Most tests will skip unless a MongoDB database is available either on the
default localhost and port or on an alternate `host:port` specified by the
`MONGOD` environment variable:

    $ export MONGOD=localhosts:31017

You can download a free, community edition of MongoDB from
[MongoDB Downloads](https://www.mongodb.org/downloads).

To build and test (after configuration):

    $ make
    $ make test
