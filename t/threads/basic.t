use strict;
use warnings;
use Test::More;

use MongoDB;
use Try::Tiny;
use threads;

my $conn = try {
    MongoDB::Connection->new({
        host => exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost',
    });
}
catch {
    plan skip_all => $_;
};

my $col = $conn->get_database('moo')->get_collection('kooh');
$col->drop;

{
    my $ret = try {
        threads->create(sub {
            $col->insert({ foo => 42 }, { safe => 1 });
        })->join->value;
    }
    catch {
        diag $_;
    };

    ok $ret, 'we survived destruction of a cloned connection';

    my $o = $col->find_one({ foo => 42 });
    is $ret, $o->{_id}, 'we inserted and joined the OID back';
}

{
    my @threads = map {
        threads->create(sub {
            my $col = $conn->get_database('moo')->get_collection('kooh');
            $col->insert({ foo => threads->self->tid }, { safe => 1 });
        })
    } 0 .. 9;

    my @vals = map { $_->tid } @threads;
    my @ids = map { $_->join } @threads;

    is scalar keys %{ { map { ($_ => 1) } @ids } }, scalar @ids,
        'we got 10 unique OIDs';

    is_deeply(
        [map { $col->find_one({ _id => $_ })->{foo} } @ids],
        [@vals],
        'right values inserted from threads',
    );
}

END {
    if ($conn) {
        $conn->moo->drop;
    }
}

done_testing;
