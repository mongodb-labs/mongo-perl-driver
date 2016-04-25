PERL DRIVER BENCHMARKING
========================

Introduction
------------

This directory implements the "MongoDB Driver Benchmarking" suite.

Contents
--------

* README.md -- this file
* bin/bench.pl -- program to run benchmarks or profiling
* cpanfile -- list of dependencies for benchmarking
* lib/BenchBSON.pm -- BSON benchmark test definitions
* lib/BenchMulti.pm -- Multi-document benchmark test definitions
* lib/BenchParallel.pm -- Multi-process benchmark test definitions
* lib/BenchSingle.pm -- Single-document benchmark test definitions

Configuration
-------------

Install dependencies in the `cpanfile`:

    cd devel/bench
    cpanm --installdeps .

Be sure to disable `PERL_MONGO_WITH_ASSERTS` in the shell environment
before running benchmarks.

Data files
----------

Benchmark datasets currently live on Google Drive in the "Drivers Hub"
folder.  They need to be copied to a local directory for use by the
benchmarking program.

Within the data file directory, files are expected to be named as follows:

* EXTENDED_BSON/deep_bson.json
* EXTENDED_BSON/flat_bson.json
* EXTENDED_BSON/full_bson.json
* SINGLE_DOCUMENT/TWEET.json
* SINGLE_DOCUMENT/SMALL_DOC.json
* SINGLE_DOCUMENT/LARGE_DOC.json
* SINGLE_DOCUMENT/GRIDFS_LARGE
* PARALLEL/GRIDFS_MULTI/file0.txt .. file49.txt
* PARALLEL/LDJSON_MULTI/LDJSON001.txt .. LDJSON100.txt

Benchmarking
------------

Benchmarking cases are provided on the command line.  The case names are:

* FlatBSONEncode
* DeepBSONEncode
* FullBSONEncode
* FlatBSONDecode
* DeepBSONDecode
* FullBSONDecode
* RunCommand
* FindOneByID
* SmallDocInsertOne
* LargeDocInsertOne
* FindManyAndEmptyCursor
* SmallDocBulkInsert
* LargeDocBulkInsert
* GridFSUploadOne
* GridFSDownloadOne
* JSONMultiImport
* JSONMultiExport
* GridFSMultiImport
* GridFSMultiExport

If none are specified, then all cases are run.

To run the benchmarks against the current commit checked out in the repo,
change to the repo root, build the driver and run benchmarks as follows:

```
$ perl Makefile.PL
$ make && $ perl -Mblib devel/bench/bin/bench.pl -d <data-dir> [cases...]
```

Use the `-v` flag for more verbose output.  Use the `--host=...` option
to set the MongoDB URI (if not set, defaults to the MONGOD environment
variable or else localhost).  Use the `-f` flag for a faster (less accurate)
benchmark run.

Profiling
---------

To profile, follow the instructions in the `Benchmarking` section, but set
the `DO_PROFILE` environment variable to 1.  (Note: the `-f` option is
recommended.) Profile data will be saved to `nytprof.out`.  Convert to an
HTML report with `nytprofhtml`.
