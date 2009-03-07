package Mongo::OID;

use Mouse;

has value => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
