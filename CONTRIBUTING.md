# Contributing Guidelines

## Introduction
`mongo-perl-driver` is the official client-side driver for talking to MongoDB with Perl. 
It is free software released under the Apache 2.0 license and available on CPAN under the
distribution name `MongoDB`.

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

