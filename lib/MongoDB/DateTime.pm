package MongoDB::DateTime;

use strict;
use warnings;
use DateTime;

use overload
  '0+'     => sub { $_[0]->{epoch} },
  '""'     => sub { $_[0]->{epoch} },
  fallback => 1;

sub new {
    return shift->from_epoch( epoch => time );
}

sub from_epoch {
    my $class = shift;
    my %arg = @_ == 1 ? %{$_[0]} : @_;
    bless \%arg, $class;
}

sub epoch { return shift->{epoch} }

sub millisecond { 0 }

sub dt {
    my $self = shift;
    return DateTime->from_epoch( epoch => $self->epoch );
}

1;

__END__

=head1 NAME

MongoDB::DateTime

=head1 SYNOPSIS

  my $dt = MongoDB::DateTime->from_epoch( epoch => time() );
  $conn->Foo->Bar->insert({ date => $dt });

=head1 DESCRIPTION

The original L<MongoDB> driver uses the L<DateTime> module to read and write dates
to Mongo. This can be a considerable bottleneck for web applications due to the
slow speed of L<DateTime>. This module implements a very light datetime object, based
on the system c<time>.

=head1 ATTRIBUTES

=head2 epoch

Returns the time in seconds since the UTC epoch.

=head2 dt

Returns a L<DateTime> object created from the current object

=head2 millisecond

Always returns C<0>. This only exists for compatibility.

=head1 SUBROUTINES

=head2 new

Creates a new object and initializes it with the current time.

=head2 from_epoch

  my $dt = MongoDB::DateTime->from_epoch( epoch => time - 3600 );

Creates a datetime object with an arbitrary time.

=head1 AUTHOR

  minimalist <minimalist@lavabit.com>

