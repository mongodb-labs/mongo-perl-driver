#line 1
use strict;
use warnings;

package Module::Install::PRIVATE::Mongo;

use Module::Install::Base;
use Config;
use File::Spec::Functions qw/catdir/;

use vars qw{$VERSION @ISA};
BEGIN {
    $VERSION = '0.45';
    @ISA     = qw{Module::Install::Base};
}

sub mongo {
    my ($self, @mongo_vars) = @_;
    my $custom_cflags = 0;
    my $ccflags = $self->makemaker_args->{CCFLAGS};

    if ($Config{osname} eq 'darwin') {
        my @arch = $Config::Config{ccflags} =~ m/-arch\s+(\S+)/g;
        my $archStr = join '', map { " -arch $_ " } @arch;

        $ccflags = $ccflags . $archStr;
        $self->makemaker_args(CCFLAGS => $ccflags);

        $self->makemaker_args(
            dynamic_lib => {
                OTHERLDFLAGS => $archStr
            }
        );

        $ccflags = $ccflags . ' -g -pipe -fno-common -DPERL_DARWIN -no-cpp-precomp -fno-strict-aliasing -Wdeclaration-after-statement -I/usr/local/include';
        $self->makemaker_args( LDDLFLAGS => ' -bundle -undefined dynamic_lookup -L/usr/local/lib');

        $custom_cflags = 1;
    }

    # check for big-endian
    my $endianess = $Config{byteorder};
    if ($endianess == 4321 || $endianess == 87654321) {
        $ccflags .= " -DMONGO_BIG_ENDIAN=1 ";

        $custom_cflags = 1;
    }

    if ($custom_cflags) {
        $self->makemaker_args( CCFLAGS => $ccflags);
    }

    $self->xs_files;

    $self->makemaker_args( INC   => '-I. ' );
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

