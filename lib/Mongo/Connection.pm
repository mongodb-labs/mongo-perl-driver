package Mongo::Connection;

use Mouse;

has host => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'localhost',
);

has port => (
    is       => 'ro',
    isa      => 'Int',
    required => 1,
    default  => 27017,
);

has _server => (
    is       => 'ro',
    isa      => 'Str',
    lazy     => 1,
    builder  => '_build__server',
);

has auto_reconnect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 0,
);

has auto_connect => (
    is       => 'ro',
    isa      => 'Bool',
    required => 1,
    default  => 1,
);

sub _build__server {
    my ($self) = @_;
    my ($host, $port) = map { $self->$_ } qw/host port/;
    return "${host}:${port}";
}

sub BUILD {
    my ($self) = @_;
    $self->_build_xs;
    $self->connect if $self->auto_connect;
}

sub connect {
    my ($self) = @_;
    $self->_connect;
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
