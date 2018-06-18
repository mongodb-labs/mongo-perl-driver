package MongoDBTest::Callback;

use Moo;
use Storable qw/ dclone /;

has events => (
  is => 'lazy',
  default => sub { [] },
  clearer => 1,
);

sub callback {
  my $self = shift;
  return sub { push @{ $self->events }, dclone $_[0] };
}

sub count {
  my $self = shift;
  return scalar( @{ $self->events } );
}

1;
