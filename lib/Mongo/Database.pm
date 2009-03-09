package Mongo::Database;

use Mouse;

has _connection => (
    is       => 'ro',
    isa      => 'Mongo::Connection',
    required => 1,
    handles  => [qw/query find_one insert update remove ensure_index/],
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has _collection_class => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
    default  => 'Mongo::Collection',
);

sub BUILD {
    my ($self) = @_;
    Mouse::load_class($self->_collection_class);
}

around qw/query find_one insert update remove ensure_index/ => sub {
    my ($next, $self, $ns, @args) = @_;
    return $self->$next($self->_query_ns($ns), @args);
};

sub _query_ns {
    my ($self, $ns) = @_;
    my $name = $self->name;
    return qq{${name}.${ns}};
}

sub collection_names {
    my ($self) = @_;
    my $it = $self->query('system.namespaces', {}, 0, 0);
    return map {
        substr($_, length($self->name) + 1)
    } map { $_->{name} } $it->all;
}

sub get_collection {
    my ($self, $collection_name) = @_;
    return $self->_collection_class->new(
        _database => $self,
        name      => $collection_name,
    );
}

sub drop {
    my ($self) = @_;
    return $self->run_command({ dropDatabase => 1 });
}

sub run_command {
    my ($self, $command) = @_;
    my $obj = $self->find_one('$cmd', $command);
    return $obj if $obj->{ok};
    confess $obj->{errmsg};
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
