# Installation Instructions for the MongoDB Perl Driver

## Supported platforms

The driver requires Perl v5.8.4 or later for most Unix-like platforms.

The driver may not build successfully on the following platforms:

* Windows
* OpenBSD (single-threaded perls without libpthread compiled in)
* Solaris

We expect to provide support for these platforms in a future release.

## Configuration requirements

Configuration requires the following Perl modules:

* Config::AutoConf
* Path::Tiny

If you are using a modern CPAN client (anything since Perl v5.12), these will
be installed automatically as needed.  If you have an older CPAN client or are
doing manual installation, install these before running `Makefile.PL`.

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
them all.  You can then `make`, etc. as usual:

    $ make
    $ make test
    $ make install

## Installing from the git repository

If you have checked out the git repository (or downloaded a tarball from
Github), you will need to install configuration requirements and follow the
manual procedure described above.

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

