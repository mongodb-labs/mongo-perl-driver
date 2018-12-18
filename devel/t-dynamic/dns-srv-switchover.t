use strict;
use Test::More;
use Test::Warnings ':all';

use lib "devel/lib";

BEGIN {
  # lowering minimum TTL value so we don't have to wait a minute
  $ENV{TEST_MONGO_MIN_RESCAN_FREQUENCY_MS} = 4;
}

use MongoDB::_URI;
use Test::Instance::DNS;
use File::Temp;
use Net::EmptyPort qw/ empty_port /;
use MongoDBTest::Orchestrator;

#
# DNS mock config setup
#

my $port = empty_port;

# Net::DNS
$ENV{RES_NAMESERVERS} = '127.0.0.1';
$ENV{RES_OPTIONS} = 'port:'.$port;

# local mock override for Socket connection
$ENV{TEST_MONGO_SOCKET_HOST} = 'localhost';

#
# ensuring basic URI switchover
#

my $uri;

with_srv('testdb1', 1234, sub {
  $uri = MongoDB::_URI->new(uri => 'mongodb+srv://test.example.com');
});

my $updated;

with_srv('testdb2', 1234, sub {
  $updated = $uri->check_for_changes({ fallback_ttl_sec => 4 });
});

ok $updated, 'uri was updated';

#
# URI specification conformity
#

with_srv(['testdb1', 'testdb2'], 1234, sub {
  $uri = MongoDB::_URI->new(uri => 'mongodb+srv://test.example.com');
  is_deeply $uri->hostids,
    ['testdb1.example.com:1234', 'testdb2.example.com:1234'],
    'correct initial hosts';
});

with_srv(['testdb1', 'testdb2', 'testdb3'], 1234, sub {
  ok $uri->check_for_changes, 'update detected';
  is_deeply $uri->hostids,
    [ 'testdb1.example.com:1234',
      'testdb2.example.com:1234',
      'testdb3.example.com:1234',
    ],
    'new host was added';
});

with_srv(['testdb2', 'testdb3'], 1234, sub {
  ok $uri->check_for_changes, 'update detected';
  is_deeply $uri->hostids,
    ['testdb2.example.com:1234', 'testdb3.example.com:1234'],
    'first host was removed';
});

with_srv(['testdb2', 'testdb4'], 1234, sub {
  ok $uri->check_for_changes, 'update detected';
  is_deeply $uri->hostids,
    ['testdb2.example.com:1234', 'testdb4.example.com:1234'],
    'host was replaced';
});

with_srv(['testdb5'], 1234, sub {
  ok $uri->check_for_changes, 'update detected';
  is_deeply $uri->hostids,
    ['testdb5.example.com:1234'],
    'hosts were both replaced (single)';
});

with_srv(['testdb2', 'testdb4'], 1234, sub {
  ok $uri->check_for_changes, 'update detected';
  is_deeply $uri->hostids,
    ['testdb2.example.com:1234', 'testdb4.example.com:1234'],
    'back to two hosts';
});

with_srv(['testdb5', 'testdb6'], 1234, sub {
  ok $uri->check_for_changes, 'update detected';
  is_deeply $uri->hostids,
    ['testdb5.example.com:1234', 'testdb6.example.com:1234'],
    'hosts were both replaced (multiple)';
});


#
# ensuring topology switchover
#

my $orc = MongoDBTest::Orchestrator->new(
    config_file => "devel/config/sharded-any.yml",
);
$orc->start;

my ($client, $coll, $inserted);
my $host = 'router1';

my @events;

with_srv('testdb1', $orc->get_server($host)->port, sub {

  use Test::DNS;
  my $dns = Test::DNS->new(nameservers => ['127.0.0.1']);
  $dns->object->port($port);
  $dns->is_a('testdb1.example.com', '127.0.0.1');

  $client = MongoDB->connect('mongodb+srv://test.example.com', {
    ssl => 0,
    monitoring_callback => sub {
      push @events, shift;
    },
  });
  $coll = $client->ns('test.db1');
  $inserted = $coll->insert_one({ foo => 23 });
});

@events = ();

with_srv('testdb2', $orc->get_server($host)->port, sub {
  $inserted = $coll->insert_one({ foo => 42 });
  is_connected('testdb2.example.com');
});

@events = ();

with_srv('testdb1', $orc->get_server($host)->port, sub {
  my $data = $coll->find_one({ _id => $inserted->inserted_id });
  is_connected('testdb1.example.com');
  is $data->{foo}, 42, 'correct value';
});

@events = ();

with_srv('testdb2', $orc->get_server($host)->port, sub {
  my $data = $coll->find_one({ _id => $inserted->inserted_id });
  is_connected('testdb2.example.com');
  is $data->{foo}, 42, 'correct value';
});

@events = ();

with_srv('testdb1', $orc->get_server($host)->port, sub {
  my $data = $coll->find_one({ _id => $inserted->inserted_id });
  is_not_connected('testdb1.example.com');
  is_connected('testdb2.example.com');
  is $data->{foo}, 42, 'correct value';
}, 0);

@events = ();

with_srv(undef, $orc->get_server($host)->port, sub {
  my $data;
  my $warning = warning {
    $data = $coll->find_one({ _id => $inserted->inserted_id });
  };
  is_connected('testdb2.example.com');
  is $data->{foo}, 42, 'correct value';
  like $warning, qr{test\.example\.com}, 'caught error as warning';
});





done_testing;

sub is_not_connected {
  my ($domain) = @_;
  my $count = grep {
    exists $_->{connectionId}
    &&
    $_->{connectionId} =~ qr{\A\Q$domain\E:\d+\z}
  } @events;
  ok !$count, "not connected to $domain";
}

sub is_connected {
  my ($domain) = @_;
  my $count = grep {
    exists $_->{connectionId}
    &&
    $_->{connectionId} =~ qr{\A\Q$domain\E:\d+\z}
  } @events;
  ok $count, "connected to $domain";
}

sub with_srv {
  my ($domain, $dbport, $callback, $wait) = @_;

  $wait = 5 unless defined $wait;
  do {
    my $domain = $domain || ['<none>'];
    $domain = [$domain]
      unless ref $domain;
    note("set SRV records to [@$domain], waiting for ${wait}s");
  };
  sleep $wait;

  do {
    my $zonefile = File::Temp->new;

    my @domains = ref($domain) ? @$domain : ($domain);

    print $zonefile "$_\n" for (
      q{$ORIGIN example.com.},
      q{$TTL 1s},
      q{testdb1 IN A 127.0.0.1},
      q{testdb2 IN A 127.0.0.1},
      q{testdb3 IN A 127.0.0.1},
      q{testdb4 IN A 127.0.0.1},
      q{testdb5 IN A 127.0.0.1},
      q{testdb6 IN A 127.0.0.1},
      q{ns IN A 127.0.0.1},
      q{example.com. IN NS ns},
      (map {
        sprintf(
          q{_mongodb._tcp.test.example.com. 1 IN SRV 0 5 %s %s},
          $dbport,
          $_,
        )
      } grep defined, @domains),
    );
    $zonefile->close;

    my $t_i_dns = Test::Instance::DNS->new(
      listen_addr => '127.0.0.1',
      listen_port => $port,
      zone_file => $zonefile->filename,
    );
    $t_i_dns->run;

    $callback->();
  };
}

