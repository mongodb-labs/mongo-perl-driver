# Contributing Guidelines

## Introduction
`mongo-perl-driver` is the official client-side driver for talking to MongoDB with Perl. 
It is free software released under the Apache 2.0 license and available on CPAN under the
distribution name `MongoDB`.

## Installation

See [INSTALL.md](INSTALL.md) for more detailed installation instructions.

## How to Ask for Help

If you are having difficulty building the driver after reading the below instructions, please email the [mongodb-user mailing list](https://groups.google.com/forum/#!forum/mongodb-user) to ask for help. Please include in your email **all** of the following information:

 - The version of the driver you are trying to build (branch or tag).
   - Examples: _maint-v0 branch_, _v0.704.2.0 tag_
 - The output of _perl -V_
 - How your version of perl was built or installed.
   - Examples: _plenv_, _perlbrew_, _built from source_
 - The error you encountered. This may be compiler, Config::AutoConf, or other output.

Failure to include the relevant information will result in additional round-trip communications to ascertain the necessary details, delaying a useful response.

## How to Contribute
The code for `mongo-perl-driver` is hosted on GitHub at:

   https://github.com/mongodb/mongo-perl-driver/

If you would like to contribute code, documentation, tests, or bugfixes, follow these steps:

1. Fork the project on GitHub.
2. Clone the fork to your local machine.
3. Make your changes and push them back up to your GitHub account.
4. Send a "pull request" with a brief description of your changes, and a link to a JIRA 
ticket if there is one.

If you are unfamiliar with GitHub, start with their excellent documentation here:

  https://help.github.com/articles/fork-a-repo

## Working with the Repository
You will need to install Config::AutoConf and Path::Tiny to be able to run
the Makefile.PL.  While this distribution is shipped using Dist::Zilla, you
do not need to install it or use it for testing.

    $ cpan Config::AutoConf Path::Tiny
    $ perl Makefile.PL
    $ make
    $ make test

