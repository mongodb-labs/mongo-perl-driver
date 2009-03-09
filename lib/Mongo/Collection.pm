package Mongo::Collection;

use Mouse;

has _database => (
    is       => 'ro',
    isa      => 'Mongo::Database',
    required => 1,
    handles  => [qw/query find_one insert update remove ensure_index/],
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

around qw/query find_one insert update remove ensure_index/ => sub {
    my ($next, $self, @args) = @_;
    return $self->$next($self->_query_ns, @args);
};

sub _query_ns {
    my ($self) = @_;
    return $self->name;
}

sub count {
    my ($self, $query) = @_;
    $query ||= {};
    my $obj = $self->_database->run_command({
        count => $self->name,
        query => $query,
    });
    return $obj->{n};
}

sub validate {
    my ($self, $scan_data) = @_;
    $scan_data = 0 unless defined $scan_data;
    my $obj = $self->_database->run_command({ validate => $self->name });
}

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
