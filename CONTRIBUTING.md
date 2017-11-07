# Contributing Guidelines

This file describes requirements and procedures for developing and testing the
MongoDB Perl driver from its code repository.  For instructions installing
from CPAN or tarball, see the [INSTALL.md](INSTALL.md) file instead.

While this distribution is shipped using Dist::Zilla, you do not need to
install it or use it for development and testing.

## Introduction

`mongo-perl-driver` is the official client-side driver for talking to
MongoDB with Perl.  It is free software released under the Apache 2.0
license and available on CPAN under the distribution name `MongoDB`.

## How to Ask for Help

If you are having difficulty building the driver after reading the
instructions here, please email the [mongodb-user mailing
list](https://groups.google.com/forum/#!forum/mongodb-user) to ask for
help. Please include in your email **all** of the following information:

 - The version of the driver you are trying to build (branch or tag).
   - Examples: _maint-v0 branch_, _v0.704.2.0 tag_
 - The output of _perl -V_
 - How your version of perl was built or installed.
   - Examples: _plenv_, _perlbrew_, _built from source_
 - The error you encountered. This may be compiler, Config::AutoConf, or other output.

Failure to include the relevant information will result in additional
round-trip communications to ascertain the necessary details, delaying a
useful response.

## Bugs / Feature Requests

Think you've found a bug? Want to see a new feature? Please open a
ticket in our issue management tool, JIRA:

 - [Create a JIRA
   account](https://jira.mongodb.org/secure/Signup!default.jspa) if you
   don't already have one.
 - Navigate to the [JIRA PERL
   project](https://jira.mongodb.org/browse/PERL)
 - Click the **Create** button - Please provide as much information as
   possible for a bug and how to reproduce it.

## How to Contribute

The code for `mongo-perl-driver` is hosted on GitHub at:

   https://github.com/mongodb/mongo-perl-driver/

If you would like to contribute code, documentation, tests, or bugfixes,
follow these steps:

1. Fork the project on GitHub.
2. Clone the fork to your local machine.
3. Make your changes and push them back up to your GitHub account.
4. Send a "pull request" with a brief description of your changes, and a
   link to a JIRA ticket if there is one.

If you are unfamiliar with GitHub, start with their excellent documentation here:

  https://help.github.com/articles/fork-a-repo

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
