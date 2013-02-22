use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;

my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::MongoClient->new(host => $host, ssl => $ENV{MONGO_SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 2;
}

my $ref = MongoDB::DBRef->new( db => 'test', ref => 'test_coll', id => 123 );
ok $ref;
isa_ok $ref, 'MongoDB::DBRef';

