package Mongo::Database;

use Mouse;

has _connection => (
    is       => 'ro',
    isa      => 'Mongo::Connection',
    required => 1,
    handles  => [qw/query find_one insert/],
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

around qw/query find_one insert/ => sub {
    my ($next, $self, $ns, @args) = @_;
    return $self->$next($self->_query_ns($ns), @args);
};

sub _query_ns {
    my ($self, $ns) = @_;
    my $name = $self->name;
    return qq{${name}.${ns}};
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
