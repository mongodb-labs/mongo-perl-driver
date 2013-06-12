package Module::Install::Compiler;

use strict;
use File::Basename        ();
use Module::Install::Base ();

use vars qw{$VERSION @ISA $ISCORE};
BEGIN {
	$VERSION = '1.06';
	@ISA     = 'Module::Install::Base';
	$ISCORE  = 1;
}

sub ppport {
	my $self = shift;
	if ( $self->is_admin ) {
		return $self->admin->ppport(@_);
	} else {
		# Fallback to just a check
		my $file = shift || 'ppport.h';
		unless ( -f $file ) {
			die "Packaging error, $file is missing";
		}
	}
}

sub cc_files {
	require Config;
	my $self = shift;
	$self->makemaker_args(
		OBJECT => join ' ', map { substr($_, 0, -2) . $Config::Config{_o} } @_
	);
}

sub cc_inc_paths {
	my $self = shift;
	$self->makemaker_args(
		INC => join ' ', map { "-I$_" } @_
	);
}

sub cc_lib_paths {
	my $self = shift;
	$self->makemaker_args(
		LIBS => join ' ', map { "-L$_" } @_
	);
}

sub cc_lib_links {
	my $self = shift;
	$self->makemaker_args(
		LIBS => join ' ', $self->makemaker_args->{LIBS}, map { "-l$_" } @_
	);
}

sub cc_optimize_flags {
	my $self = shift;
	$self->makemaker_args(
		OPTIMIZE => join ' ', @_
	);
}

1;

__END__

=pod

=head1 NAME

Module::Install::Compiler - Commands for interacting with the C compiler

=head1 SYNOPSIS

  To be completed

=head1 DESCRIPTION

Many Perl modules that contains C and XS code have fiendishly complex
F<Makefile.PL> files, because L<ExtUtils::MakeMaker> doesn't itself provide
a huge amount of assistance and automation in this area.

B<Module::Install::Compiler> provides a number of commands that take care
of common utility tasks, and try to take some of intricacy out of creating
C and XS modules.

=head1 COMMANDS

To be completed

=head1 TO DO

The current implementation is relatively fragile and minimalistic.

It only handles some very basic wrapper around L<ExtUtils::MakeMaker>.

It is currently undergoing extensive refactoring to provide a more
generic compiler flag generation capability. This may take some time,
and if anyone who maintains a Perl module that makes use of the compiler
would like to help out, your assistance would be greatly appreciated.

=head1 SEE ALSO

L<Module::Install>, L<ExtUtils::MakeMaker>

=head1 AUTHORS

Refactored by Adam Kennedy E<lt>adamk@cpan.orgE<gt>

Mostly by Audrey Tang E<lt>autrijus@autrijus.orgE<gt>

Based on original works by Brian Ingerson E<lt>ingy@cpan.orgE<gt>

=head1 COPYRIGHT

Copyright 2002, 2003, 2004, 2006 by Adam Kennedy, Audrey Tang, Brian Ingerson.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.

See L<http://www.perl.com/perl/misc/Artistic.html>

=cut
