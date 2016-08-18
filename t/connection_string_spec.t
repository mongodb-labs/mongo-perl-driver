#
#  Copyright 2016 MongoDB, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

use strict;
use warnings;
use JSON::MaybeXS;
use Path::Tiny 0.054; # basename with suffix
use Test::Fatal;
use Test::More;

use MongoDB::_URI;

sub _is_ipv4 {
    my $host = shift;

    my @octets = ( $host =~ /^(\d+)\.(\d+)\.(\d+)\.(\d+)$/ );
    return scalar( grep { $_ < 256 } @octets ) == 4;
}

sub run_test {
    my $test = shift;

    # we don't support UNIX domain sockets or IPv6 literals, so treat tests containing these
    # as if they were "valid: false" tests
    my $unsupported_hosts =
      grep { $_ eq "unix" || $_ eq "ip_literal" } map { $_->{type} } @{ $test->{hosts} };
    my $valid = $test->{valid} && $unsupported_hosts == 0;

    if ( !$valid ) {
        isnt( exception { MongoDB::_URI->new( uri => $test->{uri} ) }, undef,
            "invalid uri" );
        return;
    }

    my ( $uri, $warning_counter );
    $warning_counter = 0;
    {
        local $SIG{__WARN__} = sub { ++$warning_counter; };
        $uri = new_ok( "MongoDB::_URI", [ uri => $test->{uri} ], "uri construction" );
    }

    my @hosts;
    for my $hostid ( @{ $uri->hostids } ) {
        my ( $host, $port ) = split ":", $hostid, 2;
        my $type = _is_ipv4($host) ? "ipv4" : "hostname";
        push @hosts,
          {
            host => $host,
            port => $port,
            type => $type
          };
    }
    # for hosts without a port, the test files expect a null port, but we parse these hosts are
    # having port 27017
    $test->{hosts} = [ map { $_->{port} ||= 27017; $_ } @{ $test->{hosts} } ];

    is_deeply( \@hosts, $test->{hosts}, "parsing of host list" );

    is( $uri->db_name, $test->{auth}->{db} || "", "parsing of auth database" );
    is( $uri->username, $test->{auth}->{username}, "parsing of username" );
    is( $uri->password, $test->{auth}->{password}, "parsing of password" );

    is_deeply( $uri->options, $test->{options} || {}, "parsing of options" );

    is( !!($warning_counter > 0), !!$test->{warning}, "correct number of warnings" );
}

my $dir      = path("t/data/connection_string");
my $iterator = $dir->iterator;
while ( my $path = $iterator->() ) {
    next unless $path =~ /\.json$/;
    my $plan = eval { decode_json( $path->slurp_utf8 ) };
    if ($@) {
        die "Error decoding $path: $@";
    }
    subtest $path => sub {
        for my $test ( @{ $plan->{tests} } ) {
            my $description = $test->{description};
            # TODO PERL-654: re-enable the below test
            next
              if $path eq "t/data/connection_string/valid-auth.json"
              && $description eq "Escaped username (GSSAPI)";
            subtest $description => sub { run_test($test); }
        }
      }
}

done_testing;
