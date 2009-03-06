package Mongo::Database;

use Mouse;

has _connection => (
    is       => 'ro',
    isa      => 'Mongo::Connection',
    required => 1,
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
