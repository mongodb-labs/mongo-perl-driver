use strict;
use warnings;
use Test::More;

use MongoDB;
use Try::Tiny;
use threads;

my $conn = try {
    MongoDB::Connection->new({
        host => exists $ENV{MONGOD} ? $ENV{MONGOD} : 'localhost',
        ssl => $ENV{SSL}
    });
}
catch {
    plan skip_all => $_;
};

my $col = $conn->get_database('affe')->get_collection('tiger');
$col->drop;


$col->insert({ foo => 9,  bar => 3, shazbot => 1 });
$col->insert({ foo => 2,  bar => 5 });
$col->insert({ foo => -3, bar => 4 });
$col->insert({ foo => 4,  bar => 9, shazbot => 1 });


{
    my $cursor = $col->query;

    # force start of retrieval before creating threads
    $cursor->next;

    my $ret = threads->create(sub {
        $cursor->next;
    })->join;

    is_deeply $ret, $cursor->next,
        'cursors retain their position on thread cloning';
}

{
    my $cursor = threads->create(sub {
        my $cursor = $col->query;

        # force start of retrieval before returning the cursor
        $cursor->next;

        return $cursor;
    })->join;

    # cursor for comparison
    my $comp_cursor = $col->query;

    # seek as far ahead as we did within the thread
    $comp_cursor->next;

    is_deeply $cursor->next, $comp_cursor->next,
        'joining back cursors works';
}

{
    my $cursor = $col->query;

    # force start of retrieval before creating threads
    $cursor->next;

    my @threads = map {
        threads->create(sub {
            $cursor->next;
        });
    } 0 .. 9;

    my @ret = map { $_->join } @threads;

    is_deeply [@ret], [($cursor->next) x 10],
        'cursors retain their position on thread cloning';
}

{
    my @threads = map {
        threads->create(sub {
            my $cursor = $col->query;

            # force start of retrieval before returning the cursor
            $cursor->next;

            return $cursor;
        })
    } 0 .. 9;

    my @cursors = map { $_->join } @threads;

    # cursor for comparison
    my $comp_cursor = $col->query;

    # seek as far ahead as we did within the thread
    $comp_cursor->next;

    is_deeply [map { $_->next } @cursors], [($comp_cursor->next) x 10],
        'joining back cursors works';
}

END {
    if ($conn) {
        $conn->affe->drop;
    }
}

done_testing();
