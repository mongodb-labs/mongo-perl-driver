#!/usr/bin/env perl
#
#  Copyright 2016 - present MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

use 5.008001;
use strict;
use warnings;
use Benchmark::Lab -profile => $ENV{DO_PROFILE};
use FindBin;
use lib "$FindBin::Bin/../lib";

use Getopt::Long;

use lib 'lib';
use BenchBSON;
use BenchSingle;
use BenchMulti;
use BenchParallel;

my @cases = qw(
  FlatBSONEncode
  DeepBSONEncode
  FullBSONEncode
  FlatBSONDecode
  DeepBSONDecode
  FullBSONDecode

  RunCommand
  FindOneByID
  SmallDocInsertOne
  LargeDocInsertOne

  FindManyAndEmptyCursor
  SmallDocBulkInsert
  LargeDocBulkInsert
  GridFSUploadOne
  GridFSDownloadOne

  JSONMultiImport
  JSONMultiExport
  GridFSMultiImport
  GridFSMultiExport
);

my %known = map { $_ => 1 } @cases;

sub main {

    warn "Warning: PERL_MONGO_WITH_ASSERTS is true!  Benchmarks will be slow.\n"
      if $ENV{PERL_MONGO_WITH_ASSERTS};

    my ( $dir, $host, $fast, $verbose );
    GetOptions(
        "datadir|d=s" => \$dir,
        "host|h=s"    => \$host,
        "fast|f"      => \$fast,
        "verbose|v"   => \$verbose,
    );

    $host ||= $ENV{MONGOD} || "mongodb://localhost/";

    die "Data directory not specified with -d <dir>\n" unless $dir;
    die "Data directory '$dir', not found\n" unless -d $dir && -r $dir;

    my @todo = @ARGV ? @ARGV : @cases;

    my $bm = Benchmark::Lab->new(
        min_secs => ( $fast ? 5  : 60 ),
        max_secs => 300,
        max_reps => ( $fast ? 10 : 100 ),
        verbose  => $verbose,
    );

    for my $case (@todo) {
        die "Unknown benchmark $case\n" unless $known{$case};
        die "Can't find 'do_task' for $case\n" unless $case->can("do_task");

        my $context = { data_dir => $dir, host => $host };
        my $res = $bm->start( $case, $context );
        printf( "%20s %e\n", $case, $res->{percentiles}{50} );
    }

    return 0;
}

exit main();
