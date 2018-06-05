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

use MongoDB;

package BenchSingle;

use JSON::MaybeXS;
use Path::Tiny;

sub _set_context {
    my ( $context, $file, $n ) = @_;
    $context->{mc} = MongoDB->connect( $context->{host}, { dt_type => 'Time::Moment' } );
    my $db = $context->{db} = $context->{mc}->db("perftest");
    $db->drop;

    $context->{doc} = _load_json("$context->{data_dir}/SINGLE_DOCUMENT/$file")
      if $file;
    $context->{n} = $n
      if $n;
}

sub _load_json {
    my ($path) = @_;
    my $doc = decode_json( path($path)->slurp_utf8 );
}

sub teardown {
    my $context = shift;
    $context->{db}->drop;
}

#--------------------------------------------------------------------------#

package RunCommand;

sub setup {
    my $context = shift;
    $context->{mc} = MongoDB->connect( $context->{host}, { dt_type => 'Time::Moment' } );
    $context->{db} = $context->{mc}->db("admin");
}

sub do_task {
    my $context = shift;
    state $db = $context->{db};
    $db->run_command( [ ismaster => 1 ] ) for 1 .. 10_000;
}

#--------------------------------------------------------------------------#

package SingleDocInsert;

sub before_task {
    my $context = shift;
    my $coll = $context->{coll} = $context->{db}->coll("corpus");
    $coll->drop;
    $context->{db}->run_command( [ create => 'corpus' ] );
}

sub do_task {
    my $context = shift;
    state $coll = $context->{coll};
    state $doc  = $context->{doc};
    $coll->insert_one($doc) for 1 .. $context->{n};
}

#--------------------------------------------------------------------------#

package FindOneByID;

our @ISA = qw/BenchSingle/;

sub setup {
    my $context = shift;
    BenchSingle::_set_context( $context, "TWEET.json" );
    my %doc  = %{ $context->{doc} };
    my $coll = $context->{db}->coll("corpus");
    $coll->insert_many( [ map { { _id => $_, %doc } } 1 .. 10_000 ] );
}

sub before_task {
    my $context = shift;
    $context->{coll} = $context->{db}->coll("corpus");
}

sub do_task {
    my $context = shift;
    state $coll = $context->{coll};
    state $doc;
    $doc = $coll->find_id($_) for 1 .. 10_000;
}

#--------------------------------------------------------------------------#

package SmallDocInsertOne;

our @ISA = qw/BenchSingle SingleDocInsert/;

sub setup {
    my $context = shift;
    BenchSingle::_set_context( $context, "SMALL_DOC.json", 10_000 );
}

#--------------------------------------------------------------------------#

package LargeDocInsertOne;

our @ISA = qw/BenchSingle SingleDocInsert/;

sub setup {
    my $context = shift;
    BenchSingle::_set_context( $context, "LARGE_DOC.json", 10 );
}

1;
