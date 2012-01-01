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
    $conn = MongoDB::Connection->new(host => $host, ssl => $ENV{SSL});
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 11;
}

my $db   = $conn->get_database('test_database');

my $result = $db->run_command({reseterror => 1});
is($result->{ok}, 1, 'reset error');

$result = $db->last_error;
is($result->{ok}, 1, 'last_error1');
is($result->{n}, 0, 'last_error2');
is($result->{err}, undef, 'last_error3');

$result = $db->run_command({forceerror => 1});
ok($result =~ /asser[st]ion/, 'forced error: '.$result);

$result = $db->last_error;
is($result->{ok}, 1, 'last_error1');
is($result->{n}, 0, 'last_error2');
is($result->{err}, 'forced error', 'last_error3');

my $hello = $db->eval('function(x) { return "hello, "+x; }', ["world"]);
is('hello, world', $hello, 'db eval');

my $err = $db->eval('function(x) { xreturn "hello, "+x; }', ["world"]);
ok($err =~ /compile failed: JS Error/, 'js err');

# tie
{
    my $admin = $conn->get_database('admin');
    my %cmd;
    tie( %cmd, 'Tie::IxHash', buildinfo => 1);
    my $result = $admin->run_command(\%cmd);
    is($result->{ok}, 1);
}

END {
    if ($db) {
        $db->drop;
    }
}
