use strict;
use warnings;
use Test::More;
use Test::Exception;

use MongoDB;

my $conn;
eval {
    $conn = MongoDB::Connection->new;
};

if ($@) {
    plan skip_all => $@;
}
else {
    plan tests => 10;
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
is('compile failed: JS Error: SyntaxError: missing ; before statement nofile_b:0', $err, 'js err');

END {
    if ($db) {
        $db->drop;
    }
}
