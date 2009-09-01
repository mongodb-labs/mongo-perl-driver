use strict;
use warnings;
use Test::More tests => 8;
use Test::Exception;

use MongoDB;

my $conn = MongoDB::Connection->new;
my $db   = $conn->get_database('test_database');

my $result = $db->run_command({reseterror => 1});
is($result->{ok}, 1, 'reset error');

$result = $db->last_error;
is($result->{ok}, 1, 'last_error1');
is($result->{n}, 0, 'last_error2');
is($result->{err}, undef, 'last_error3');

$result = $db->run_command({forceerror => 1});
is($result, 'db assertion failure', 'forced error');

$result = $db->last_error;
is($result->{ok}, 1, 'last_error1');
is($result->{n}, 0, 'last_error2');
is($result->{err}, 'forced error', 'last_error3');

