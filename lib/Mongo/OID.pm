package Mongo::OID;

use Mouse;

has value => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_value',
);

no Mouse;
__PACKAGE__->meta->make_immutable;

1;
