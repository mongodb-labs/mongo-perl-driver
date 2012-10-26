use strict;
use warnings;
use Test::More;


use MongoDB::Timestamp; # needed if db is being run as master
use MongoDB;


my $conn;
eval {
    my $host = "localhost";
    if (exists $ENV{MONGOD}) {
        $host = $ENV{MONGOD};
    }
    $conn = MongoDB::Connection->new(host => $host, ssl => $ENV{MONGO_SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 2;
}

my $db = $conn->get_database('admin');

my $ret = $db->run_command({fsync => 1});
is($ret->{ok}, 1, "fsync returned 'ok');