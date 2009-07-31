use strict;
use warnings;
use Test::More tests => 2;
use Test::Exception;

use MongoDB;
use MongoDB::OID;

my $id = MongoDB::OID->new;
isa_ok($id, 'MongoDB::OID');
is($id."", $id->value);
