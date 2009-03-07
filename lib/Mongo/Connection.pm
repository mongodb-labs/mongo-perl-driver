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

has _database_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'Mongo::Database',
);

has _cursor_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'Mongo::Cursor',
);

has _oid_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'Mongo::OID',
);

sub _build__server {
    my ($self) = @_;
    my ($host, $port) = map { $self->$_ } qw/host port/;
    return "${host}:${port}";
}

sub BUILD {
    my ($self) = @_;
    eval "use ${_}" # no Mouse::load_class becase the namespaces already have symbols from the xs bootstrap
        for map { $self->$_ } qw/_database_class _cursor_class _oid_class/;
    $self->_build_xs;
    $self->connect if $self->auto_connect;
}

sub connect {
    my ($self) = @_;
    $self->_connect;
    return;
}

sub find_one {
    my ($self, @args) = @_;
    $self->_find_one(@args);
}

sub database_names {
    my ($self) = @_;
    my $ret = $self->find_one('admin.$cmd' => { listDatabases => 1 });
    return map { $_->{name} } @{ $ret->{databases} };
}

sub get_database {
    my ($self, $database_name) = @_;
    return $self->_database_class->new(
        _connection => $self,
        name        => $database_name,
    );
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
