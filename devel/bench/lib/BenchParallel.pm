use v5.10;
use strict;
use warnings;

package BenchParallel;

use base qw/BenchSingle/; # for teardown

use Parallel::ForkManager;

sub parallel_mongodb {
    my ( $context, $jobs, $fcn ) = @_;

    my $pm = Parallel::ForkManager->new( $jobs > 1 ? $jobs : 0 );

    local $SIG{INT} = sub {
        warn "Caught SIGINT; Waiting for child processes\n";
        $pm->wait_all_children;
        exit 1;
    };

    for my $i ( 0 .. $jobs - 1 ) {
        $pm->start and next;
        $SIG{INT} = sub { $pm->finish };
        $context->{mc}->reconnect;
        $fcn->( $context, $jobs, $i );
        $pm->finish;
    }

    $pm->wait_all_children;
}

package JSONMultiImport;

our @ISA = qw/BenchParallel/;

use JSON::MaybeXS;
use Path::Tiny;

sub setup {
    my $context = shift;
    BenchSingle::_set_context($context);
    $context->{doc_dir} = path("$context->{data_dir}/PARALLEL/LDJSON_MULTI");
    $context->{json} = JSON::MaybeXS->new( allow_blessed => 1, convert_blessed => 1 );
}

sub before_task {
    my $context = shift;
    my $coll = $context->{coll} = $context->{db}->coll("corpus");
    $coll->drop;
    $context->{db}->run_command( [ create => 'corpus' ] );
}

sub do_task {
    my $context = shift;
    my ( $coll, $json ) = @{$context}{qw/coll json/};
    my $jobs = 4;

    BenchParallel::parallel_mongodb(
        $context, $jobs,
        sub {
            my ( $context, $jobs, $i ) = @_;
            for my $f ( sort grep { /\.txt$/ } $context->{doc_dir}->children ) {
                my ($d) = $f->basename =~ /LDJSON(\d+)\.txt/;
                next unless $d % $jobs == $i;
                my $bulk = $coll->unordered_bulk;
                for my $l ( $f->lines_utf8 ) {
                    $bulk->insert_one( $json->decode($l) );
                }
                $bulk->execute;
            }
        }
    );

}

package JSONMultiExport;

our @ISA = qw/BenchParallel/;

use JSON::MaybeXS;
use Path::Tiny;

sub setup {
    my $context = shift;
    JSONMultiImport::setup($context);

    $context->{temp_dir} = Path::Tiny->tempdir;

    JSONMultiImport::before_task($context);

    my $coll = $context->{coll};
    $coll->indexes->create_one( [ _fileid => 1 ] );

    # upload with _fileid; eventually replace with JSONMultiImport::do_task
    # when import data includes file id
    BenchParallel::parallel_mongodb(
        $context, 4,
        sub {
            my ( $context, $jobs, $i ) = @_;
            for my $f ( sort grep { /\.txt$/ } $context->{doc_dir}->children ) {
                my ($d) = $f->basename =~ /LDJSON(\d+)\.txt/;
                next unless $d % $jobs == $i;
                my $bulk = $coll->unordered_bulk;
                for my $l ( $f->lines_utf8 ) {
                    my $doc = $context->{json}->decode($l);
                    $doc->{_fileid} = 0+ $d;
                    $bulk->insert_one($doc);
                }
                $bulk->execute;
            }
        }
    );

}

sub before_task {
    my $context = shift;
    # benchmark requires this to be newly initialized
    $context->{coll} = $context->{db}->coll("corpus");
}

sub do_task {
    my $context = shift;
    my ( $coll, $json ) = @{$context}{qw/coll json/};
    my $jobs = 4;

    BenchParallel::parallel_mongodb(
        $context, $jobs,
        sub {
            my ( $context, $jobs, $i ) = @_;
            for my $fid ( grep { $_ % $jobs == $i } 1 .. 100 ) {
                my $out = $context->{temp_dir}->child( sprintf( "LDJSON%03d.txt", $fid ) );
                $out->spew(
                    join( "\n",
                        map { $json->encode($_) } $coll->find( { _fileid => $fid } )->result->all )
                );
            }
        }
    );

}

package GridFSMultiImport;

our @ISA = qw/BenchParallel/;

use Path::Tiny;

sub setup {
    my $context = shift;
    BenchSingle::_set_context($context);
    $context->{doc_dir} = path("$context->{data_dir}/PARALLEL/GRIDFS_MULTI");
}

sub before_task {
    my $context = shift;
    BenchMulti::_gridfs_reset($context);
}

sub do_task {
    my $context = shift;
    my $gfs     = $context->{gfs};
    my $jobs    = 4;

    BenchParallel::parallel_mongodb(
        $context, $jobs,
        sub {
            my ( $context, $jobs, $i ) = @_;
            for my $f ( sort grep { /\.txt$/ } $context->{doc_dir}->children ) {
                my ($d) = $f->basename =~ /file(\d+)\.txt/;
                next unless $d % $jobs == $i;
                my $fh = $f->openr_raw;
                $gfs->upload_from_stream( $f->basename, $fh );
            }
        }
    );

}

package GridFSMultiExport;

our @ISA = qw/BenchParallel/;

use Path::Tiny;

sub setup {
    my $context = shift;
    GridFSMultiImport::setup($context);
    GridFSMultiImport::before_task($context);
    GridFSMultiImport::do_task($context);

    $context->{temp_dir} = Path::Tiny->tempdir;
}

sub before_task {
    my $context = shift;
    # benchmark wants fresh object
    $context->{gfs} = $context->{db}->gfs;
}

sub do_task {
    my $context = shift;
    my $gfs     = $context->{gfs};
    my $jobs    = 4;

    my @files = grep { defined $_->[0] }
      map { $_->{filename} =~ /^file(\d+)\.txt$/; [ $1, $_->{_id} ] } $gfs->find()->all;

    BenchParallel::parallel_mongodb(
        $context, $jobs,
        sub {
            my ( $context, $jobs, $i ) = @_;
            for my $f (@files) {
                my ( $digits, $file_id ) = @$f;
                next unless $digits % $jobs == $i;
                my $out = $context->{temp_dir}->child( sprintf( "file%02d.txt", $digits ) );
                my $fh = $out->openw_raw;
                $gfs->download_to_stream( $file_id, $fh );
            }
        }
    );

}

1;
