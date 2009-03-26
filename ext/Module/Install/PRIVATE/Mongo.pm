use strict;
use warnings;

package Module::Install::PRIVATE::Mongo;

use Module::Install::Base;
use Config;
use File::Spec::Functions qw/catdir/;

use vars qw{$VERSION @ISA};
BEGIN {
    $VERSION = '0.01';
    @ISA     = qw{Module::Install::Base};
}

sub mongo {
    my ($self, $mongo_sdk) = @_;

    my $cc;
    if ($ENV{CC}) {
        $cc = $ENV{CC};
    } elsif ($Config{gccversion} and $Config{cc}  =~ m{\bgcc\b[^/]*$}) {
        ($cc = $Config{cc}) =~ s[\bgcc\b([^/]*)$(?:)][g\+\+$1];
    } elsif ($Config{osname} =~ /^MSWin/) {
        $cc = 'cl -TP';
    } elsif ($Config{osname} eq 'linux') {
        $cc = 'g++';
    } elsif ($Config{osname} eq 'cygwin') {
        $cc = 'g++';
    } elsif ($Config{osname} eq 'solaris' or $Config{osname} eq 'SunOS') {
        if ($Config{cc} eq 'gcc') {
            $cc = 'g++';
        } else {
            $cc = 'CC';
        }
    } else {
        $cc = 'g++';
    }

    $self->requires_external_bin($cc);;
    $self->xs_files;

    $self->makemaker_args( INC   => '-I. -I/usr/include/boost -I' . catdir($mongo_sdk, 'include') );
    $self->makemaker_args( CC    => $cc );
    $self->makemaker_args( XSOPT => ' -C++' );
    $self->cc_lib_paths(catdir($mongo_sdk, 'lib'));
    $self->cc_lib_links(qw/mongoclient boost_thread-mt boost_filesystem-mt boost_program_options-mt/);

    return;
}

sub xs_files {
    my ($self) = @_;
    my (@clean, @OBJECT, %XS);

    for my $xs (<xs/*.xs>) {
        (my $c = $xs) =~ s/\.xs$/.c/i;
        (my $o = $xs) =~ s/\.xs$/\$(OBJ_EXT)/i;

        $XS{$xs} = $c;
        push @OBJECT, $o;
        push @clean, $o;
    }

    for my $c (<*.c>) {
        (my $o = $c) =~ s/\.c$/\$(OBJ_EXT)/i;

        push @OBJECT, $o;
        push @clean, $o;
    }

    $self->makemaker_args(
        clean  => { FILES => join(q{ }, @clean) },
        OBJECT => join(q{ }, @OBJECT),
        XS     => \%XS,
    );

    $self->postamble('$(OBJECT) : perl_mongo.h');

    return;
}

1;
