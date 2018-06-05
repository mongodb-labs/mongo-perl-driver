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

use v5.10;
use strict;
use warnings;

package BenchMulti;

use base qw/BenchSingle/; # for teardown

sub _gridfs_reset {
    my $context = shift;
    my $gfs = $context->{gfs} = $context->{db}->gfs;
    $gfs->drop;
    my $fh = $gfs->open_upload_stream("onebyte");
    $fh->print("a");
    $fh->close;
}

#--------------------------------------------------------------------------#

package FindManyAndEmptyCursor;

our @ISA = qw/BenchMulti/;

sub setup {
    my $context = shift;
    BenchSingle::_set_context( $context, "TWEET.json" );
    my $coll = $context->{db}->coll("corpus");
    $coll->insert_many( [ map { $context->{doc} } 1 .. 10_000 ] );
}

sub before_task {
    my $context = shift;
    $context->{coll} = $context->{db}->coll("corpus");
}

sub do_task {
    my $context = shift;
    state $coll = $context->{coll};
    $coll->find( {} )->result->all;
}

#--------------------------------------------------------------------------#

package MultiDocInsert;

sub before_task {
    my $context = shift;
    my $coll = $context->{coll} = $context->{db}->coll("corpus");
    $coll->drop;
    $context->{db}->run_command( [ create => 'corpus' ] );
}

sub do_task {
    my $context = shift;
    state $coll = $context->{coll};
    $coll->insert_many( $context->{docs} );
}

#--------------------------------------------------------------------------#

package SmallDocBulkInsert;

our @ISA = qw/BenchMulti MultiDocInsert/;

sub setup {
    my $context = shift;
    BenchSingle::_set_context( $context, "SMALL_DOC.json" );
    $context->{docs} = [ map { $context->{doc} } 1 .. 10_000 ];
}

#--------------------------------------------------------------------------#

package LargeDocBulkInsert;

our @ISA = qw/BenchMulti MultiDocInsert/;

sub setup {
    my $context = shift;
    BenchSingle::_set_context( $context, "LARGE_DOC.json" );
    $context->{docs} = [ map { $context->{doc} } 1 .. 10 ];
}

#--------------------------------------------------------------------------#

package GridFSUploadOne;

our @ISA = qw/BenchMulti/;

use Path::Tiny;

sub setup {
    my $context = shift;
    BenchSingle::_set_context($context);
    $context->{doc} =
      path("$context->{data_dir}/SINGLE_DOCUMENT/GRIDFS_LARGE")->slurp_raw;
}

sub before_task {
    my $context = shift;
    BenchMulti::_gridfs_reset($context);
}

sub do_task {
    my $context = shift;
    state $gfs = $context->{gfs};
    my $fh = $gfs->open_upload_stream("GRIDFS_LARGE");
    $fh->print( $context->{doc} );
    $fh->close;
}

#--------------------------------------------------------------------------#

package GridFSDownloadOne;

our @ISA = qw/BenchMulti/;

use Path::Tiny;

sub setup {
    my $context = shift;
    BenchSingle::_set_context($context);
    BenchMulti::_gridfs_reset($context);
    my $fh =
      path("$context->{data_dir}/SINGLE_DOCUMENT/GRIDFS_LARGE")->openr_raw();
    $context->{doc_id} = $context->{gfs}->upload_from_stream("GRIDFS_LARGE", $fh);
}

sub before_task {
    my $context = shift;
    $context->{gfs} = $context->{db}->gfs;
}

sub do_task {
    my $context = shift;
    state $gfs = $context->{gfs};
    state $id = $context->{doc_id};
    my $fh = $gfs->open_download_stream($id);
    do { local $/; $fh->readline() };
    $fh->close;
}

1;
