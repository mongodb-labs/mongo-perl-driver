package Mongo::OID;

use Any::Moose;

has value => (
    is      => 'ro',
    isa     => 'Str',
    lazy    => 1,
    builder => '_build_value',
);

no Any::Moose;
__PACKAGE__->meta->make_immutable;

1;
