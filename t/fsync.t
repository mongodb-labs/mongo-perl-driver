use strict;
use warnings;
use Test::More;

use Data::Dumper;

use MongoDB::Timestamp; # needed if db is being run as master
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
    plan tests => 11;
}

# Test normal fsync.
my $ret = $conn->fsync();
is($ret->{ok},              1, "fsync returned 'ok' => 1");
is(exists $ret->{numFiles}, 1, "fsync returned 'numFiles'");

# Test async fsync.
$ret = $conn->fsync({async => 1});
SKIP: { 
    $ret =~ s/exception: //, warn($ret), skip $ret, 2 if $ret =~ /not supported/;

    is($ret->{ok},              1, "fsync + async returned 'ok' => 1");
    is(exists $ret->{numFiles}, 1, "fsync + async returned 'numFiles'");
}

# Test fsync with lock.
$ret = $conn->fsync({lock => 1});
is($ret->{ok},              1, "fsync + lock returned 'ok' => 1");
is(exists $ret->{seeAlso},  1, "fsync + lock returned a link to fsync+lock documentation.");
is($ret->{info}, "now locked against writes, use db.fsyncUnlock() to unlock", "Successfully locked mongodb.");

# Check the lock.
$ret = $conn->get_database('admin')->get_collection('$cmd.sys.inprog')->find_one();
is($ret->{fsyncLock}, 1, "MongoDB is still locked.");
is($ret->{info}, "use db.fsyncUnlock() to terminate the fsync write/snapshot lock", "Got docs on how to unlock (via shell).");

# Unlock 
$ret = $conn->fsync_unlock(); Dumper($ret);
is($ret->{ok}, 1, "Got 'ok' => 1 from unlock command.");
is($ret->{info}, "unlock completed", "Got a successful unlock.");

