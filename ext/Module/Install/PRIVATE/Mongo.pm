use strict;
use warnings;

package Module::Install::PRIVATE::Mongo;

use Module::Install::Base;
use Config;
use File::Spec::Functions qw/catdir/;

use vars qw{$VERSION @ISA};
BEGIN {
    $VERSION = '0.43';
    @ISA     = qw{Module::Install::Base};
}

# check for big-endian                                                                                                                                                                                                                
my $endianess = $Config{byteorder};
my $ccflags = "";
if ($endianess == 4321 || $endianess == 87654321) {
    $ccflags = " -DMONGO_BIG_ENDIAN=1 ";
}

sub mongo {
    my ($self, @mongo_vars) = @_;

    if ($Config{osname} eq 'darwin') {
        $ccflags = $ccflags . ' -g -pipe -fno-common -DPERL_DARWIN -no-cpp-precomp -fno-strict-aliasing -Wdeclaration-after-statement -I/usr/local/include';
        $self->makemaker_args( LDDLFLAGS => ' -bundle -undefined dynamic_lookup -L/usr/local/lib');
    }

    $self->makemaker_args( CCFLAGS => $ccflags);
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
