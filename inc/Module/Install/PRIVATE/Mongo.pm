use strict;
use warnings;

package Module::Install::PRIVATE::Mongo;

use Module::Install::Base;

our @ISA = qw{Module::Install::Base};

sub extratargets {
    my ($self) = @_;

    $self->postamble(<<'HERE');

cover : pure_all
	HARNESS_PERL_SWITCHES=-MDevel::Cover make test

ptest : pure_all
	HARNESS_OPTIONS=j9 make test

HERE

    return;
}

1;

